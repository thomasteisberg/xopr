"""
STAC catalog creation utilities for Open Polar Radar data.
"""

import re
import json
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

import numpy as np
import pyarrow.parquet as pq
import pystac
import stac_geoparquet
from dask.distributed import LocalCluster, Client
from shapely.geometry import mapping
from dask.distributed import as_completed

from .metadata import extract_item_metadata, discover_campaigns, discover_flight_lines, collect_uniform_metadata
from .geometry import (
    simplify_geometry_polar_projection,
    merge_item_geometries,
    merge_flight_geometries,
    build_collection_extent_and_geometry,
    build_collection_extent
)
from .config import CatalogConfig, config_from_kwargs

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.3.0/schema.json'
PROJ_EXT = 'https://stac-extensions.github.io/projection/v2.0.0/schema.json'


def create_catalog(
    catalog_id: str = "OPR",
    description: str = "Open Polar Radar airborne data",
    stac_extensions: Optional[List[str]] = None
) -> pystac.Catalog:
    """
    Create a root STAC catalog for OPR data.
    
    Parameters
    ----------
    catalog_id : str, default "OPR"
        Unique identifier for the catalog.
    description : str, default "Open Polar Radar airborne data"
        Human-readable description of the catalog.
    stac_extensions : list of str, optional
        List of STAC extension URLs to enable. If None, defaults to
        projection and file extensions.
        
    Returns
    -------
    pystac.Catalog
        Root catalog object.
        
    Examples
    --------
    >>> catalog = create_catalog("MyOPR", "My Open Polar Radar data")
    >>> collection = create_collection("2016_Antarctica", "2016 flights", extent)
    >>> catalog.add_child(collection)
    >>> catalog.save("./output")
    """
    if stac_extensions is None:
        stac_extensions = [
            'https://stac-extensions.github.io/projection/v2.0.0/schema.json',
            'https://stac-extensions.github.io/file/v2.1.0/schema.json',
        ]
    
    return pystac.Catalog(
        id=catalog_id,
        description=description,
        stac_extensions=stac_extensions
    )


def create_collection(
    collection_id: str,
    description: str,
    extent: pystac.Extent,
    license: str = "various",
    stac_extensions: Optional[List[str]] = None,
    geometry: Optional[Dict[str, Any]] = None
) -> pystac.Collection:
    """
    Create a STAC collection for a campaign or data product grouping.
    
    Parameters
    ----------
    collection_id : str
        Unique identifier for the collection.
    description : str
        Human-readable description of the collection.
    extent : pystac.Extent
        Spatial and temporal extent of the collection.
    license : str, default ""
        Data license identifier.
    stac_extensions : list of str, optional
        List of STAC extension URLs to enable. If None, defaults to
        empty list.
    geometry : dict, optional
        GeoJSON geometry object for the collection. If provided, the
        projection extension will be added automatically.
        
    Returns
    -------
    pystac.Collection
        Collection object.
        
    Examples
    --------
    >>> from datetime import datetime
    >>> import pystac
    >>> extent = pystac.Extent(
    ...     spatial=pystac.SpatialExtent([[-180, -90, 180, 90]]),
    ...     temporal=pystac.TemporalExtent([[datetime(2016, 1, 1), datetime(2016, 12, 31)]])
    ... )
    >>> collection = create_collection("2016_campaign", "2016 Antarctic flights", extent)
    >>> item = create_item("item_001", geometry, bbox, datetime.now())
    >>> collection.add_item(item)
    """
    if stac_extensions is None:
        stac_extensions = []
    
    # Add projection extension if geometry is provided
    if geometry is not None and PROJ_EXT not in stac_extensions:
        stac_extensions = stac_extensions + [PROJ_EXT]
    
    collection = pystac.Collection(
        id=collection_id,
        description=description,
        extent=extent,
        license=license,
        stac_extensions=stac_extensions
    )
    
    # Add geometry to extra_fields if provided
    if geometry is not None:
        collection.extra_fields['proj:geometry'] = geometry
    
    return collection


def create_item(
    item_id: str,
    geometry: Dict[str, Any],
    bbox: List[float],
    datetime: Any,
    properties: Optional[Dict[str, Any]] = None,
    assets: Optional[Dict[str, pystac.Asset]] = None,
    stac_extensions: Optional[List[str]] = None
) -> pystac.Item:
    """
    Create a STAC item for a flight line data segment.
    
    Parameters
    ----------
    item_id : str
        Unique identifier for the item.
    geometry : dict
        GeoJSON geometry object.
    bbox : list of float
        Bounding box coordinates [xmin, ymin, xmax, ymax].
    datetime : datetime
        Acquisition datetime.
    properties : dict, optional
        Additional metadata properties. If None, defaults to empty dict.
    assets : dict of str to pystac.Asset, optional
        Dictionary of assets (data files, thumbnails, etc.). Keys are 
        asset names, values are pystac.Asset objects.
    stac_extensions : list of str, optional
        List of STAC extension URLs to enable. If None, defaults to
        file extension.
        
    Returns
    -------
    pystac.Item
        Item object with specified properties and assets.
        
    Examples
    --------
    >>> from datetime import datetime
    >>> import pystac
    >>> geometry = {"type": "Point", "coordinates": [-71.0, 42.0]}
    >>> bbox = [-71.1, 41.9, -70.9, 42.1]
    >>> props = {"instrument": "radar", "platform": "aircraft"}
    >>> assets = {
    ...     "data": pystac.Asset(href="https://example.com/data.mat", media_type="application/octet-stream")
    ... }
    >>> item = create_item("flight_001", geometry, bbox, datetime.now(), props, assets)
    """
    if properties is None:
        properties = {}
    if stac_extensions is None:
        stac_extensions = ['https://stac-extensions.github.io/file/v2.1.0/schema.json']
    
    item = pystac.Item(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime,
        properties=properties,
        stac_extensions=stac_extensions
    )
    
    if assets:
        for key, asset in assets.items():
            item.add_asset(key, asset)
    
    return item


def create_items_from_flight_data(
    flight_data: Dict[str, Any],
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    campaign_name: str = "",
    primary_data_product: str = "CSARP_standard",
    verbose: bool = False
) -> List[pystac.Item]:
    """
    Create STAC items from flight line data.
    
    Parameters
    ----------
    flight_data : dict
        Flight metadata from discover_flight_lines(). Expected to contain
        'flight_id' and 'data_files' keys.
    base_url : str, default "https://data.cresis.ku.edu/data/rds/"
        Base URL for constructing asset hrefs.
    campaign_name : str, default ""
        Campaign name for URL construction.
    primary_data_product : str, default "CSARP_standard"
        Data product name to use as primary data source.
    verbose : bool, default False
        If True, print details for each item being processed.
        
    Returns
    -------
    list of pystac.Item
        List of STAC Item objects, one per MAT file in the flight data.
        Each item contains geometry, temporal information, and asset links.
    """
    items = []
    flight_id = flight_data['flight_id']

    primary_data_files = flight_data['data_files'][primary_data_product].values()
    
    for data_file_path in primary_data_files:
        data_path = Path(data_file_path)
        
        try:
            # Extract metadata from MAT file only (no CSV needed)
            metadata = extract_item_metadata(data_path)
        except Exception as e:
            print(f"Warning: Failed to extract metadata for {data_path}: {e}")
            continue

        item_id = f"{data_path.stem}"
        
        geometry = mapping(metadata['geom'])
        bbox = list(metadata['bbox'].bounds)
        datetime = metadata['date']

        rel_mat_path = f"{campaign_name}/{primary_data_product}/{flight_id}/{data_path.name}"
        data_href = base_url + rel_mat_path
        
        # Extract segment number from MAT filename (e.g., "Data_20161014_03_001.mat" -> "001")
        segment_match = re.search(r'_(\d+)\.mat$', data_path.name)
        segment = segment_match.group(1)
        
        # Extract date and flight number from flight_id (e.g., "20161014_03" -> "20161014", "03")
        # Split on underscore to avoid assuming fixed lengths
        parts = flight_id.split('_')
        date_part = parts[0]  # YYYYMMDD
        flight_num_str = parts[1]  # Flight number as string
        
        # Create OPR-specific properties
        properties = {
            'opr:date': date_part,
            'opr:flight': int(flight_num_str),
            'opr:segment': int(segment)
        }
        
        # Add scientific extension properties if available
        item_stac_extensions = ['https://stac-extensions.github.io/file/v2.1.0/schema.json']
        
        if metadata.get('doi') is not None:
            properties['sci:doi'] = metadata['doi']
        
        if metadata.get('citation') is not None:
            properties['sci:citation'] = metadata['citation']
        
        if metadata.get('doi') is not None or metadata.get('citation') is not None:
            item_stac_extensions.append('https://stac-extensions.github.io/scientific/v1.0.0/schema.json')
        
        # Add SAR extension properties if available
        if metadata.get('frequency') is not None:
            properties['sar:center_frequency'] = metadata['frequency']
        
        if metadata.get('bandwidth') is not None:
            properties['sar:bandwidth'] = metadata['bandwidth']
        
        if metadata.get('frequency') is not None or metadata.get('bandwidth') is not None:
            item_stac_extensions.append('https://stac-extensions.github.io/sar/v1.3.0/schema.json')
        
        assets = {}

        for data_product_type in flight_data['data_files'].keys():
            if data_path.name in flight_data['data_files'][data_product_type]:
                product_path = flight_data['data_files'][data_product_type][data_path.name]
                file_type = metadata.get('mimetype') # get_mat_file_type(product_path)
                if verbose:
                    print(f"[{file_type}] {product_path}")
                assets[data_product_type] = pystac.Asset(
                    href=base_url + f"{campaign_name}/{data_product_type}/{flight_id}/{data_path.name}",
                    media_type=file_type
                )
                if data_product_type == primary_data_product:
                    assets['data'] = assets[data_product_type]
        
        thumb_href = base_url + f"{campaign_name}/images/{flight_id}/{flight_id}_{segment}_2echo_picks.jpg"
        assets['thumbnails'] = pystac.Asset(
            href=thumb_href,
            media_type=pystac.MediaType.JPEG
        )
        
        flight_path_href = base_url + f"{campaign_name}/images/{flight_id}/{flight_id}_{segment}_0maps.jpg"
        assets['flight_path'] = pystac.Asset(
            href=flight_path_href,
            media_type=pystac.MediaType.JPEG
        )
        
        item = create_item(
            item_id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=datetime,
            properties=properties,
            assets=assets,
            stac_extensions=item_stac_extensions
        )
        
        items.append(item)
    
    return items

def build_limited_catalog(
    data_root: Path,
    output_path: Path,
    catalog_id: str = "OPR",
    data_product: str = "CSARP_standard",
    extra_data_products: list[str] = ['CSARP_layer',
                                      'CSARP_qlook'],
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_items: int = 10,
    campaign_filter: list = None,
    verbose: bool = False
) -> pystac.Catalog:
    """
    Build STAC catalog with limits for faster processing.

    Parameters
    ----------
    data_root : Path
        Root directory containing campaign data
    output_path : Path
        Directory where catalog will be saved
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    data_product : str, optional
        Primary data product to process, by default "CSARP_standard"
    extra_data_products : List[str], optional
        Additional data products to include, by default ['CSARP_layer', 'CSARP_qlook']
    base_url : str, optional
        Base URL for asset hrefs, by default "https://data.cresis.ku.edu/data/rds/"
    max_items : int, optional
        Maximum number of items to process, by default 10
    campaign_filter : List[str], optional
        Specific campaigns to process, by default None (all campaigns)
    verbose : bool, optional
        If True, print details for each item being processed, by default False

    Returns
    -------
    pystac.Catalog
        The built STAC catalog
    """
    catalog = create_catalog(catalog_id=catalog_id)
    campaigns = discover_campaigns(data_root)

    # Filter campaigns if specified
    if campaign_filter:
        campaigns = [c for c in campaigns if c['name'] in campaign_filter]

    for campaign in campaigns:
        campaign_path = Path(campaign['path'])
        campaign_name = campaign['name']

        print(f"Processing campaign: {campaign_name}")

        try:
            flight_lines = discover_flight_lines(
                campaign_path, data_product,
                extra_data_products=extra_data_products
            )
        except FileNotFoundError as e:
            print(f"Warning: Skipping {campaign_name}: {e}")
            continue

        if not flight_lines:
            print(f"Warning: No flight lines found for {campaign_name}")
            continue

        # Limit the number of flight lines processed if specified
        if max_items is not None:
            flight_lines = flight_lines[:max_items]

        # Group items by flight_id to create flight collections
        flight_collections = []
        all_campaign_items = []
        flight_geometries = []

        for flight_data in flight_lines:
            try:
                items = create_items_from_flight_data(
                    flight_data, base_url, campaign_name, data_product, verbose
                )

                # Create flight collection
                flight_id = flight_data['flight_id']
                flight_extent, flight_geometry = build_collection_extent_and_geometry(items)

                # Collect metadata from items for flight collection
                flight_extensions, flight_extra_fields = collect_uniform_metadata(
                    items,
                    ['sci:doi', 'sci:citation', 'sar:center_frequency', 'sar:bandwidth']
                )

                flight_collection = create_collection(
                    collection_id=flight_id,
                    description=(
                        f"Flight {flight_id} data from {campaign['year']} "
                        f"{campaign['aircraft']} over {campaign['location']}"
                    ),
                    extent=flight_extent,
                    stac_extensions=(
                        flight_extensions if flight_extensions else None
                    ),
                    geometry=flight_geometry
                )

                # Add scientific extra fields to flight collection
                for key, value in flight_extra_fields.items():
                    flight_collection.extra_fields[key] = value

                # Add items to flight collection
                flight_collection.add_items(items)
                flight_collections.append(flight_collection)
                all_campaign_items.extend(items)
                
                # Collect flight geometry for campaign-level merging
                if flight_geometry is not None:
                    flight_geometries.append(flight_geometry)

                print(
                    f"  Added flight collection {flight_id} with "
                    f"{len(items)} items"
                )

                # Break early if we've hit the max items limit
                if (max_items is not None and
                        len(all_campaign_items) >= max_items):
                    break

            except Exception as e:
                flight_id = flight_data['flight_id']
                print(f"Warning: Failed to process flight {flight_id}: {e}")
                continue

        if flight_collections:
            # Create campaign collection with extent covering all flights
            campaign_extent = build_collection_extent(all_campaign_items)
            
            # Merge flight geometries into campaign-level MultiLineString
            campaign_geometry = merge_flight_geometries(flight_geometries)

            # Collect metadata from all campaign items
            campaign_extensions, campaign_extra_fields = collect_uniform_metadata(
                all_campaign_items,
                ['sci:doi', 'sci:citation', 'sar:center_frequency', 'sar:bandwidth']
            )

            campaign_collection = create_collection(
                collection_id=campaign_name,
                description=(
                    f"{campaign['year']} {campaign['aircraft']} flights "
                    f"over {campaign['location']}"
                ),
                extent=campaign_extent,
                stac_extensions=(
                    campaign_extensions if campaign_extensions else None
                ),
                geometry=campaign_geometry
            )

            # Add scientific extra fields to campaign collection
            for key, value in campaign_extra_fields.items():
                campaign_collection.extra_fields[key] = value

            # Add flight collections as children of campaign collection
            for flight_collection in flight_collections:
                campaign_collection.add_child(flight_collection)

            catalog.add_child(campaign_collection)

            print(
                f"Added campaign collection {campaign_name} with "
                f"{len(flight_collections)} flight collections and "
                f"{len(all_campaign_items)} total items"
            )

    output_path.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    print(f"Catalog saved to {output_path}")
    return catalog


def build_flat_collection(
    campaign: dict,
    data_root: Path,
    config: CatalogConfig
) -> Path:
    """
    Build a single flattened STAC collection for one campaign and write to parquet.
    
    This creates a flattened collection structure: campaign -> items (no flight collections).
    Campaign collections have bbox-only extent with no geometry fields, suitable for
    parquet export to STAC servers. The collection is immediately written to parquet
    and only the path is returned to avoid memory accumulation.

    Parameters
    ----------
    campaign : dict
        Campaign metadata with 'name', 'path', 'year', 'location', 'aircraft'
    data_root : Path
        Root directory containing campaign data
    config : CatalogConfig
        Configuration object with catalog parameters. Must have output_dir set.

    Returns
    -------
    Path
        Path to the written parquet file
        
    Raises
    ------
    FileNotFoundError
        If campaign data directory is not found
    ValueError
        If no flight lines are found for the campaign
        
    Examples
    --------
    >>> from xopr.stac import discover_campaigns, CatalogConfig
    >>> campaigns = discover_campaigns(Path('/data'))
    >>> config = CatalogConfig(output_dir=Path('./output'))
    >>> parquet_path = build_flat_collection(campaigns[0], Path('/data'), config)
    >>> print(f"Exported campaign to {parquet_path}")
    """
    # Validate output_dir is set
    if config.output_dir is None:
        raise ValueError("config.output_dir must be set for build_flat_collection")
    
    campaign_path = Path(campaign['path'])
    campaign_name = campaign['name']

    if config.verbose:
        print(f"Processing campaign: {campaign_name}")

    # Discover flight lines for this campaign
    try:
        flight_lines = discover_flight_lines(
            campaign_path, config.data_product,
            extra_data_products=config.extra_data_products
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Campaign data not found for {campaign_name}: {e}")

    if not flight_lines:
        raise ValueError(f"No flight lines found for {campaign_name}")

    # Limit the number of flight lines processed if specified
    if config.max_items is not None:
        flight_lines = flight_lines[:config.max_items]

    # Collect ALL items across all flights for this campaign
    all_campaign_items = []

    for flight_data in flight_lines:
        try:
            items = create_items_from_flight_data(
                flight_data, config.base_url, campaign_name, config.data_product, config.verbose
            )
            all_campaign_items.extend(items)

            flight_id = flight_data['flight_id']
            if config.verbose:
                print(f"  Added {len(items)} items from flight {flight_id}")

            # Break early if we've hit the max items limit
            if (config.max_items is not None and
                    len(all_campaign_items) >= config.max_items):
                break

        except Exception as e:
            flight_id = flight_data['flight_id']
            if config.verbose:
                print(f"Warning: Failed to process flight {flight_id}: {e}")
            continue

    if not all_campaign_items:
        raise ValueError(f"No valid items found for {campaign_name}")

    # Create campaign collection with bbox-only extent (no geometry)
    campaign_extent = build_collection_extent(all_campaign_items)
    
    # Collect metadata from all campaign items
    campaign_extensions, campaign_extra_fields = collect_uniform_metadata(
        all_campaign_items,
        ['sci:doi', 'sci:citation', 'sar:center_frequency', 'sar:bandwidth']
    )

    # Create campaign collection with NO geometry field at all
    campaign_collection = create_collection(
        collection_id=campaign_name,
        description=(
            f"{campaign['year']} {campaign['aircraft']} flights "
            f"over {campaign['location']}"
        ),
        extent=campaign_extent,
        stac_extensions=(
            campaign_extensions if campaign_extensions else None
        )
        # Note: no geometry parameter - bbox-only for parquet compatibility
    )

    # Add scientific extra fields to campaign collection
    for key, value in campaign_extra_fields.items():
        campaign_collection.extra_fields[key] = value

    # Add ALL items directly to campaign collection (flattened structure)
    campaign_collection.add_items(all_campaign_items)
    
    if config.verbose:
        print(f"Added flattened campaign collection {campaign_name} with {len(all_campaign_items)} total items (no flight collections)")
    
    # Export to parquet and return the path (not the collection) to avoid memory accumulation
    parquet_path = export_collection_to_parquet(campaign_collection, config.output_dir, config.verbose)
    
    if not parquet_path:
        raise ValueError(f"Failed to write parquet file for campaign {campaign_name}")
    
    return parquet_path


def export_collection_to_parquet(
    collection: pystac.Collection,
    output_dir: Path,
    verbose: bool = False
) -> Optional[Path]:
    """
    Export a single STAC collection to a parquet file with collection metadata.
    
    This function directly converts STAC items to GeoParquet format without
    intermediate NDJSON, and includes the collection metadata in the Parquet
    file metadata as per the STAC GeoParquet specification.
    
    Parameters
    ----------
    collection : pystac.Collection
        STAC collection to export
    output_dir : Path
        Output directory for the parquet file
    verbose : bool, optional
        If True, print progress messages
        
    Returns
    -------
    Path or None
        Path to the created parquet file, or None if no items to export
        
    Examples
    --------
    >>> collection = build_flat_collection(campaign, data_root, config)
    >>> parquet_path = export_collection_to_parquet(collection, Path('output/'))
    >>> print(f"Exported to {parquet_path}")
    """
    # Get items from collection and subcollections
    collection_items = list(collection.get_items())
    if not collection_items:
        for child_collection in collection.get_collections():
            collection_items.extend(list(child_collection.get_items()))
    
    if not collection_items:
        if verbose:
            print(f"  Skipping {collection.id}: no items")
        return None
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Export to parquet
    parquet_file = output_dir / f"{collection.id}.parquet"
    
    if verbose:
        print(f"  Exporting collection: {collection.id} ({len(collection_items)} items)")
    
    # Build collections metadata - single collection in this case
    collection_dict = collection.to_dict()
    # Clean collection links - remove item links with None hrefs
    if 'links' in collection_dict:
        collection_dict['links'] = [
            link for link in collection_dict['links']
            if not (link.get('rel') == 'item' and link.get('href') is None)
        ]
    collections_dict = {
        collection.id: collection_dict
    }
    
    # Clean items before export - remove links with None hrefs
    # These are added by PySTAC when items are added to collections but have no physical location
    clean_items = []
    for item in collection_items:
        item_dict = item.to_dict()
        if 'links' in item_dict:
            item_dict['links'] = [
                link for link in item_dict['links']
                if link.get('href') is not None
            ]
        clean_items.append(item_dict)
    
    # Convert items to Arrow format
    record_batch_reader = stac_geoparquet.arrow.parse_stac_items_to_arrow(clean_items)
    
    # Write to Parquet with collection metadata
    # Note: Using collection_metadata for compatibility with stac-geoparquet 0.7.0
    # In newer versions (>0.8), this should be 'collections' parameter
    stac_geoparquet.arrow.to_parquet(
        table=record_batch_reader,
        output_path=parquet_file,
        collection_metadata=collection_dict,  # Single collection metadata (cleaned)
        schema_version="1.1.0",  # Use latest schema version
        compression="snappy",  # Use snappy compression for better performance
        write_statistics=True  # Write column statistics for query optimization
    )
    
    if verbose:
        size_kb = parquet_file.stat().st_size / 1024
        print(f"  ✅ {collection.id}.parquet saved ({size_kb:.1f} KB)")
    
    return parquet_file


def build_catalog_from_parquet_files(
    parquet_paths: List[Path],
    catalog_id: str = "OPR",
    catalog_description: str = "Open Polar Radar airborne data",
    config: Optional[CatalogConfig] = None
) -> pystac.Catalog:
    """
    Build a STAC catalog from existing parquet files.
    
    This function reads collections from parquet files using stac_geoparquet
    and assembles them into a STAC catalog. Each parquet file contains exactly
    one collection.

    Parameters
    ----------
    parquet_paths : List[Path]
        List of paths to parquet files (one collection per file)
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    catalog_description : str, optional
        Catalog description, by default "Open Polar Radar airborne data"
    config : CatalogConfig, optional
        Configuration object with catalog parameters. If None, uses defaults.

    Returns
    -------
    pystac.Catalog
        The built STAC catalog with collections
        
    Examples
    --------
    >>> parquet_files = list(Path('./output').glob("*.parquet"))
    >>> catalog = build_catalog_from_parquet_files(
    ...     parquet_files, config=CatalogConfig(verbose=True)
    ... )
    """
    # Handle config
    if config is None:
        config = CatalogConfig()
    
    catalog = create_catalog(catalog_id=catalog_id, description=catalog_description)
    
    if config.verbose:
        print(f"Building catalog from {len(parquet_paths)} parquet files")
    
    for parquet_path in parquet_paths:
        try:
            # Get collection metadata from parquet file metadata (without reading the data)
            parquet_metadata = pq.read_metadata(str(parquet_path))
            file_metadata = parquet_metadata.metadata
            
            # Check if collections metadata is in the file metadata
            # For stac-geoparquet 0.7.0, check for 'stac-geoparquet' key
            collection_dict = None
            if file_metadata:
                # Try new format first (for future compatibility)
                if b'stac:collections' in file_metadata:
                    collections_json = file_metadata[b'stac:collections'].decode('utf-8')
                    collections_data = json.loads(collections_json)
                    # Get the first (and should be only) collection
                    if collections_data:
                        collection_id = list(collections_data.keys())[0]
                        collection_dict = collections_data[collection_id]
                # Try legacy format used by stac-geoparquet 0.7.0
                elif b'stac-geoparquet' in file_metadata:
                    geoparquet_meta = json.loads(file_metadata[b'stac-geoparquet'].decode('utf-8'))
                    if 'collection' in geoparquet_meta:
                        collection_dict = geoparquet_meta['collection']
            
            if not collection_dict:
                raise ValueError(
                    f"No STAC collection metadata found in parquet file: {parquet_path.name}. "
                    f"The parquet file must include collection metadata in the 'stac:collections' "
                    f"field of the file metadata."
                )
            
            # Reconstruct collection from metadata (without items - they stay in parquet)
            collection = pystac.Collection.from_dict(collection_dict)
            
            # Add collection to catalog (items remain in parquet file)
            catalog.add_child(collection)
            
            if config.verbose:
                # Get row count from metadata without reading the table
                num_rows = parquet_metadata.num_rows
                print(f"  ✅ Added collection: {collection.id} from {parquet_path.name} ({num_rows} items in parquet)")
                    
        except Exception as e:
            if config.verbose:
                print(f"  ❌ Failed to process {parquet_path.name}: {e}")
            continue
    
    if config.verbose:
        collections = list(catalog.get_collections())
        print(f"Built catalog with {len(collections)} collections")
    
    return catalog


def build_flat_catalog_dask(
    data_root: Path,
    output_path: Path,
    catalog_id: str = "OPR",
    catalog_description: str = "Open Polar Radar airborne data",
    campaign_filter: list = None,
    config: Optional[CatalogConfig] = None,
    **kwargs  # For backward compatibility
) -> pystac.Catalog:
    """
    Build flattened STAC catalog using Dask for parallel campaign processing.
    
    This function processes campaigns in parallel using Dask LocalCluster,
    with each worker writing parquet files immediately to avoid memory accumulation.
    The catalog is then built from the parquet files.

    Parameters
    ----------
    data_root : Path
        Root directory containing campaign data
    output_path : Path
        Directory where catalog and parquet files will be saved
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    catalog_description : str, optional
        Catalog description, by default "Open Polar Radar airborne data"
    campaign_filter : list, optional
        Specific campaigns to process, by default None (all campaigns)
    config : CatalogConfig, optional
        Configuration object with catalog parameters. If None, uses defaults.
    **kwargs
        For backward compatibility - individual parameters can be passed

    Returns
    -------
    pystac.Catalog
        The built flattened STAC catalog
        
    Examples
    --------
    >>> config = CatalogConfig(n_workers=8, verbose=True, output_dir=Path('./output'))
    >>> catalog = build_flat_catalog_dask(
    ...     Path('/data'), Path('./output'), config=config
    ... )
    >>> # Parquet files are written during processing to ./output/<campaign>.parquet
    """
    # Handle backward compatibility and config
    config = config_from_kwargs(config, **kwargs)
    
    # Set output_dir in config if not already set
    if config.output_dir is None:
        config = config.copy_with(output_dir=output_path)
    
    # Discover campaigns
    campaigns = discover_campaigns(data_root)
    
    # Filter campaigns if specified
    if campaign_filter:
        campaigns = [c for c in campaigns if c['name'] in campaign_filter]
    
    print(f"🚀 Processing {len(campaigns)} campaigns with Dask workers")
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Set up Dask cluster
    cluster = LocalCluster(
        memory_limit=config.memory_limit,
        n_workers=config.n_workers,
        threads_per_worker=config.threads_per_worker
    )
    client = Client(cluster)
    
    try:
        print(f"   Dashboard: {client.dashboard_link}")
        
        # Submit campaign processing tasks - will return parquet paths
        futures_to_campaigns = {}
        # Create a config for workers with verbose=False to reduce noise
        worker_config = config.copy_with(verbose=False)
        for campaign in campaigns:
            future = client.submit(
                build_flat_collection,
                campaign=campaign,
                data_root=data_root,
                config=worker_config
            )
            futures_to_campaigns[future] = campaign
            
        if config.verbose:
            print(f"   Submitted {len(futures_to_campaigns)} campaign tasks")
            print(f"   Parquet files will be written to: {output_path}/<campaign>.parquet")
        
        # Process results as they complete and collect parquet paths
        completed_count = 0
        failed_campaigns = []
        parquet_paths = []
        
        for future in as_completed(futures_to_campaigns):
            completed_count += 1
            campaign = futures_to_campaigns[future]
            campaign_name = campaign['name']
            
            try:
                # Get the parquet path from the completed future
                parquet_path = future.result()
                parquet_paths.append(parquet_path)
                
                print(f"   ✅ [{completed_count}/{len(futures_to_campaigns)}] Completed: {campaign_name} → {parquet_path.name}")
                
            except Exception as e:
                failed_campaigns.append(campaign_name)
                print(f"   ❌ [{completed_count}/{len(futures_to_campaigns)}] Failed: {campaign_name} - {e}")
        
        if failed_campaigns:
            print(f"\n   ⚠️  {len(failed_campaigns)} campaigns failed: {', '.join(failed_campaigns)}")
            
    finally:
        # Clean up cluster
        client.close()
        cluster.close()
    
    # Build catalog from the parquet files
    catalog = build_catalog_from_parquet_files(
        parquet_paths=parquet_paths,
        catalog_id=catalog_id,
        catalog_description=catalog_description,
        config=config
    )
    
    # Save the catalog.json
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    if config.verbose:
        successful = len(parquet_paths)
        print(f"\n🎉 Flattened catalog saved to {output_path}/catalog.json")
        print(f"   Processed {successful}/{len(campaigns)} campaigns successfully")
        print(f"   Campaign parquet files: {output_path}/<campaign>.parquet")
    
    return catalog
