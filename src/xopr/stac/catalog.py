"""
STAC catalog creation utilities for Open Polar Radar data.
"""

import re
import json
import logging
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
from omegaconf import DictConfig, OmegaConf

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
    config: DictConfig,
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    campaign_name: str = "",
    primary_data_product: str = "CSARP_standard",
    verbose: bool = False,
    error_log_file: Optional[Union[str, Path]] = None
) -> List[pystac.Item]:
    """
    Create STAC items from flight line data.
    
    Parameters
    ----------
    flight_data : dict
        Flight metadata from discover_flight_lines(). Expected to contain
        'flight_id' and 'data_files' keys.
    config : DictConfig
        Configuration object with geometry.tolerance setting for simplification.
    base_url : str, default "https://data.cresis.ku.edu/data/rds/"
        Base URL for constructing asset hrefs.
    campaign_name : str, default ""
        Campaign name for URL construction.
    primary_data_product : str, default "CSARP_standard"
        Data product name to use as primary data source.
    verbose : bool, default False
        If True, print details for each item being processed.
    error_log_file : str or Path, optional
        Path to file where metadata extraction errors will be logged.
        If None, errors are printed to stdout (default behavior).
        
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
            error_msg = f"Failed to extract metadata for {data_path}: {e}"
            
            if error_log_file is not None:
                # Log to file
                with open(error_log_file, 'a', encoding='utf-8') as f:
                    f.write(f"{error_msg}\n")
            else:
                # Fallback to print (current behavior)
                print(f"Warning: {error_msg}")
            
            continue

        item_id = f"{data_path.stem}"
        
        # Simplify geometry using config tolerance
        simplified_geom = simplify_geometry_polar_projection(
            metadata['geom'], 
            simplify_tolerance=config.geometry.tolerance
        )
        geometry = mapping(simplified_geom)
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

def export_collection_to_parquet(
    collection: pystac.Collection,
    config: DictConfig
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
    config : DictConfig
        Configuration object with output.path and logging.verbose settings
        
    Returns
    -------
    Path or None
        Path to the created parquet file, or None if no items to export
        
    Examples
    --------
    >>> from omegaconf import OmegaConf
    >>> config = OmegaConf.create({'output': {'path': './output'}, 'logging': {'verbose': True}})
    >>> parquet_path = export_collection_to_parquet(collection, config)
    >>> print(f"Exported to {parquet_path}")
    """
    # Extract settings from config
    output_dir = Path(config.output.path)
    verbose = config.logging.get('verbose', False)
    
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
    config: DictConfig
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
    config : DictConfig
        Configuration object with output.catalog_id, output.catalog_description,
        and logging.verbose settings

    Returns
    -------
    pystac.Catalog
        The built STAC catalog with collections
        
    Examples
    --------
    >>> from omegaconf import OmegaConf
    >>> parquet_files = list(Path('./output').glob("*.parquet"))
    >>> config = OmegaConf.create({
    ...     'output': {'catalog_id': 'OPR', 'catalog_description': 'Open Polar Radar data'},
    ...     'logging': {'verbose': True}
    ... })
    >>> catalog = build_catalog_from_parquet_files(parquet_files, config)
    """
    # Extract settings from config
    catalog_id = config.output.get('catalog_id', 'OPR')
    catalog_description = config.output.get('catalog_description', 'Open Polar Radar airborne data')
    verbose = config.logging.get('verbose', False)
    
    catalog = create_catalog(catalog_id=catalog_id, description=catalog_description)
    if verbose:
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
            
            if verbose:
                # Get row count from metadata without reading the table
                num_rows = parquet_metadata.num_rows
                print(f"  ✅ Added collection: {collection.id} from {parquet_path.name} ({num_rows} items in parquet)")
                    
        except Exception as e:
            if verbose:
                print(f"  ❌ Failed to process {parquet_path.name}: {e}")
            continue
    
    if verbose:
        collections = list(catalog.get_collections())
        print(f"Built catalog with {len(collections)} collections")
    
    return catalog
