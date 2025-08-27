"""
STAC catalog creation utilities for Open Polar Radar data.
"""

import re
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

import numpy as np
import pystac
from dask.distributed import LocalCluster, Client

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
    config: Optional[CatalogConfig] = None,
    **kwargs  # For backward compatibility
) -> pystac.Collection:
    """
    Build a single flattened STAC collection for one campaign.
    
    This creates a flattened collection structure: campaign -> items (no flight collections).
    Campaign collections have bbox-only extent with no geometry fields, suitable for
    parquet export to STAC servers.

    Parameters
    ----------
    campaign : dict
        Campaign metadata with 'name', 'path', 'year', 'location', 'aircraft'
    data_root : Path
        Root directory containing campaign data
    config : CatalogConfig, optional
        Configuration object with catalog parameters. If None, uses defaults.
    **kwargs
        For backward compatibility - individual parameters can be passed:
        data_product, extra_data_products, base_url, max_items, verbose

    Returns
    -------
    pystac.Collection
        The built flattened STAC collection for the campaign
        
    Raises
    ------
    FileNotFoundError
        If campaign data directory is not found
    ValueError
        If no flight lines are found for the campaign
        
    Examples
    --------
    >>> from xopr.stac import discover_campaigns
    >>> campaigns = discover_campaigns(Path('/data'))
    >>> collection = build_flat_collection(campaigns[0], Path('/data'))
    >>> print(f"Built collection {collection.id} with {len(list(collection.get_items()))} items")
    
    >>> # Export collection to parquet
    >>> from xopr.stac.build import export_collections_to_parquet
    >>> catalog = create_catalog()
    >>> catalog.add_child(collection)
    >>> files = export_collections_to_parquet(catalog, Path('./output'))
    >>> print(f"Exported to {files[collection.id]}")
    """
    # Handle backward compatibility and config
    config = config_from_kwargs(config, **kwargs)
    
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
    
    return campaign_collection


def build_flat_catalog(
    campaigns: list[dict],
    catalog_id: str = "OPR",
    catalog_description: str = "Open Polar Radar airborne data",
    config: Optional[CatalogConfig] = None,
    **kwargs  # For backward compatibility
) -> pystac.Catalog:
    """
    Build flattened STAC catalog from a list of campaign collections.
    
    This creates a simplified structure without flight collections: catalog -> campaigns -> items.
    Campaign collections have bbox-only extent with no geometry fields, suitable for
    parquet export to STAC servers.

    Parameters
    ----------
    campaigns : list[dict]
        List of campaign dictionaries with 'name', 'path', 'year', 'location', 'aircraft'
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    catalog_description : str, optional
        Catalog description, by default "Open Polar Radar airborne data"
    config : CatalogConfig, optional
        Configuration object with catalog parameters. If None, uses defaults.
    **kwargs
        For backward compatibility - individual parameters can be passed:
        data_product, extra_data_products, base_url, max_items, verbose

    Returns
    -------
    pystac.Catalog
        The built flattened STAC catalog
        
    Examples
    --------
    >>> from xopr.stac import discover_campaigns
    >>> campaigns = discover_campaigns(Path('/data'))
    >>> catalog = build_flat_catalog(campaigns[:2], verbose=True)
    >>> catalog.normalize_and_save('output')
    """
    # Handle backward compatibility and config
    config = config_from_kwargs(config, **kwargs)
    
    catalog = create_catalog(catalog_id=catalog_id, description=catalog_description)
    
    for campaign in campaigns:
        try:
            collection = build_flat_collection(
                campaign=campaign,
                data_root=Path('.'),  # Not used since campaign has full path
                config=config
            )
            catalog.add_child(collection)
            
        except (FileNotFoundError, ValueError) as e:
            if config.verbose:
                print(f"Warning: Skipping {campaign['name']}: {e}")
            continue
    
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
    then assembles them into a flat catalog structure.

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
    extra_data_products : list[str], optional
        Additional data products to include, by default ['CSARP_layer']
    base_url : str, optional
        Base URL for asset hrefs, by default "https://data.cresis.ku.edu/data/rds/"
    max_items : int, optional
        Maximum number of items per campaign, by default None (all items)
    campaign_filter : list, optional
        Specific campaigns to process, by default None (all campaigns)
    n_workers : int, optional
        Number of Dask workers, by default 4
    memory_limit : str, optional
        Memory limit per worker, by default 'auto'
    verbose : bool, optional
        If True, print details for each item being processed, by default False

    Returns
    -------
    pystac.Catalog
        The built flattened STAC catalog
        
    Examples
    --------
    >>> catalog = build_flat_catalog_dask(
    ...     Path('/data'), Path('./output'), 
    ...     n_workers=8, verbose=True
    ... )
    >>> # Export to parquet
    >>> from xopr.stac.build import export_to_geoparquet
    >>> export_to_geoparquet(catalog, Path('./output/catalog.parquet'))
    """
    # Discover campaigns
    campaigns = discover_campaigns(data_root)
    
    # Filter campaigns if specified
    if campaign_filter:
        campaigns = [c for c in campaigns if c['name'] in campaign_filter]
    
    if verbose:
        print(f"🚀 Processing {len(campaigns)} campaigns with {n_workers} Dask workers")
    
    # Set up Dask cluster
    cluster = LocalCluster(memory_limit=memory_limit, n_workers=n_workers)
    client = Client(cluster)
    
    try:
        if verbose:
            print(f"   Dashboard: {client.dashboard_link}")
        
        # Submit campaign processing tasks
        futures = []
        for campaign in campaigns:
            future = client.submit(
                build_flat_collection,
                campaign=campaign,
                data_root=data_root,
                data_product=data_product,
                extra_data_products=extra_data_products,
                base_url=base_url,
                max_items=max_items,
                verbose=False  # Disable verbose for workers to reduce noise
            )
            futures.append(future)
            
        if verbose:
            print(f"   Submitted {len(futures)} campaign tasks")
        
        # Collect results
        collections = []
        for i, future in enumerate(futures):
            try:
                collection = future.result()
                collections.append(collection)
                if verbose:
                    print(f"   ✅ [{i+1}/{len(futures)}] Completed: {collection.id}")
            except Exception as e:
                campaign_name = campaigns[i]['name']
                if verbose:
                    print(f"   ❌ [{i+1}/{len(futures)}] Failed: {campaign_name} - {e}")
        
        # Build catalog from successful collections
        catalog = create_catalog(catalog_id=catalog_id, description="Open Polar Radar airborne data")
        for collection in collections:
            catalog.add_child(collection)
            
    finally:
        # Clean up cluster
        client.close()
        cluster.close()
    
    # Save the catalog to specified output path
    output_path.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    if config.verbose:
        print(f"🎉 Flattened catalog saved to {output_path}")
        print(f"   Processed {len(collections)} campaigns successfully")
    return catalog
