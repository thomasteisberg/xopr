"""
STAC catalog building functions for Open Polar Radar data.

This module provides core functions for building STAC catalogs from OPR data,
including processing campaigns, flights, and items. These functions are designed
to be testable and reusable, supporting both sequential and parallel execution.
"""

import json
from pathlib import Path
from typing import List, Optional, Dict, Any

import numpy as np
import pystac
import stac_geoparquet

from .catalog import (
    create_catalog, create_collection,
    build_collection_extent, create_items_from_flight_data,
    build_collection_extent_and_geometry, merge_flight_geometries
)
from .metadata import discover_campaigns, discover_flight_lines

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.3.0/schema.json'


# ============================================================================
# Core Processing Functions
# ============================================================================

def process_single_flight(
    flight_data: Dict[str, Any],
    base_url: str,
    campaign_name: str,
    primary_data_product: str,
    campaign_info: Dict[str, Any],
    verbose: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Process a single flight and return flight collection data.
    
    Parameters
    ----------
    flight_data : dict
        Flight metadata from discover_flight_lines() with 'flight_id' and 'data_files'
    base_url : str
        Base URL for asset hrefs (e.g., "https://data.cresis.ku.edu/data/rds/")
    campaign_name : str
        Campaign name (e.g., "2016_Antarctica_DC8")
    primary_data_product : str
        Primary data product name (e.g., "CSARP_standard")
    campaign_info : dict
        Campaign metadata with 'year', 'location', 'aircraft'
    verbose : bool, optional
        If True, print verbose output
        
    Returns
    -------
    dict or None
        Dictionary with keys:
        - 'collection': pystac.Collection for the flight
        - 'items': list of pystac.Item objects
        - 'geometry': flight geometry as GeoJSON dict
        - 'flight_id': flight identifier string
        Returns None if processing fails.
    
    Examples
    --------
    >>> flight_data = {
    ...     'flight_id': '20161014_03',
    ...     'data_files': {'CSARP_standard': {...}}
    ... }
    >>> result = process_single_flight(
    ...     flight_data, 
    ...     "https://example.com/", 
    ...     "2016_Antarctica_DC8",
    ...     "CSARP_standard",
    ...     {'year': '2016', 'location': 'Antarctica', 'aircraft': 'DC8'}
    ... )
    >>> if result:
    ...     print(f"Processed flight {result['flight_id']} with {len(result['items'])} items")
    """
    try:
        # Create items for this flight
        items = create_items_from_flight_data(
            flight_data, base_url, campaign_name, primary_data_product, verbose
        )
        
        if not items:
            return None
        
        flight_id = flight_data['flight_id']
        flight_extent, flight_geometry = build_collection_extent_and_geometry(items)
        
        # Collect metadata for extensions
        flight_extensions, flight_extra_fields = collect_metadata_from_items(items)
        
        # Create flight collection
        flight_collection = create_collection(
            collection_id=flight_id,
            description=(
                f"Flight {flight_id} data from {campaign_info['year']} "
                f"{campaign_info['aircraft']} over {campaign_info['location']}"
            ),
            extent=flight_extent,
            stac_extensions=flight_extensions if flight_extensions else None,
            geometry=flight_geometry
        )
        
        # Add extra fields
        for key, value in flight_extra_fields.items():
            flight_collection.extra_fields[key] = value
        
        # Add items to collection
        flight_collection.add_items(items)
        
        return {
            'collection': flight_collection,
            'items': items,
            'geometry': flight_geometry,
            'flight_id': flight_id
        }
        
    except Exception as e:
        flight_id = flight_data.get('flight_id', 'unknown')
        if verbose:
            print(f"Warning: Failed to process flight {flight_id}: {e}")
        return None


def process_single_campaign(
    campaign: Dict[str, Any],
    data_root: Path,
    data_product: str = "CSARP_standard",
    extra_data_products: Optional[List[str]] = None,
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_flights: Optional[int] = None,
    verbose: bool = False
) -> Optional[pystac.Collection]:
    """
    Process a single campaign and return campaign collection.
    
    Parameters
    ----------
    campaign : dict
        Campaign metadata with keys 'name', 'path', 'year', 'location', 'aircraft'
    data_root : Path
        Root directory containing campaign data
    data_product : str, optional
        Primary data product to process
    extra_data_products : list of str, optional
        Additional data products to include
    base_url : str, optional
        Base URL for asset hrefs
    max_flights : int, optional
        Maximum number of flights to process (None for all)
    verbose : bool, optional
        If True, print verbose output
        
    Returns
    -------
    pystac.Collection or None
        Campaign collection with flight subcollections, or None if processing fails
    
    Examples
    --------
    >>> campaign = {
    ...     'name': '2016_Antarctica_DC8',
    ...     'path': '/data/2016_Antarctica_DC8',
    ...     'year': '2016',
    ...     'location': 'Antarctica',
    ...     'aircraft': 'DC8'
    ... }
    >>> collection = process_single_campaign(campaign, Path('/data'))
    >>> if collection:
    ...     print(f"Processed {collection.id} with {len(list(collection.get_collections()))} flights")
    """
    if extra_data_products is None:
        extra_data_products = ['CSARP_layer']
    
    campaign_path = Path(campaign['path'])
    campaign_name = campaign['name']
    
    if verbose:
        print(f"Processing campaign: {campaign_name}")
    
    # Discover flight lines
    try:
        flight_lines = discover_flight_lines(
            campaign_path, data_product,
            extra_data_products=extra_data_products
        )
    except FileNotFoundError as e:
        if verbose:
            print(f"Warning: Skipping {campaign_name}: {e}")
        return None
    
    if not flight_lines:
        if verbose:
            print(f"Warning: No flight lines found for {campaign_name}")
        return None
    
    # Limit flights if specified
    if max_flights is not None:
        flight_lines = flight_lines[:max_flights]
    
    # Process flights
    flight_collections = []
    all_campaign_items = []
    flight_geometries = []
    
    for flight_data in flight_lines:
        flight_result = process_single_flight(
            flight_data, base_url, campaign_name, 
            data_product, campaign, verbose
        )
        
        if flight_result:
            flight_collections.append(flight_result['collection'])
            all_campaign_items.extend(flight_result['items'])
            if flight_result['geometry'] is not None:
                flight_geometries.append(flight_result['geometry'])
            
            if verbose:
                print(f"  Added flight {flight_result['flight_id']} with {len(flight_result['items'])} items")
    
    if not flight_collections:
        if verbose:
            print(f"Warning: No valid flights processed for {campaign_name}")
        return None
    
    # Create campaign collection
    campaign_extent = build_collection_extent(all_campaign_items)
    campaign_geometry = merge_flight_geometries(flight_geometries) if flight_geometries else None
    
    # Collect metadata for extensions
    campaign_extensions, campaign_extra_fields = collect_metadata_from_items(all_campaign_items)
    
    campaign_collection = create_collection(
        collection_id=campaign_name,
        description=(
            f"{campaign['year']} {campaign['aircraft']} flights "
            f"over {campaign['location']}"
        ),
        extent=campaign_extent,
        stac_extensions=campaign_extensions if campaign_extensions else None,
        geometry=campaign_geometry
    )
    
    # Add extra fields
    for key, value in campaign_extra_fields.items():
        campaign_collection.extra_fields[key] = value
    
    # Add flight collections as children
    for flight_collection in flight_collections:
        campaign_collection.add_child(flight_collection)
    
    if verbose:
        print(
            f"Completed campaign {campaign_name} with "
            f"{len(flight_collections)} flight collections and "
            f"{len(all_campaign_items)} total items"
        )
    
    return campaign_collection


def collect_metadata_from_items(items: List[pystac.Item]) -> tuple:
    """
    Collect metadata from items for STAC extension fields.
    
    Parameters
    ----------
    items : list of pystac.Item
        STAC items to extract metadata from
        
    Returns
    -------
    tuple
        (extensions, extra_fields) where:
        - extensions: list of STAC extension URLs to enable
        - extra_fields: dict of extra fields to add to collection
    
    Examples
    --------
    >>> items = [...]  # List of STAC items with properties
    >>> extensions, fields = collect_metadata_from_items(items)
    >>> print(f"Extensions: {extensions}")
    >>> print(f"Extra fields: {fields}")
    """
    extensions = []
    extra_fields = {}
    
    # Scientific metadata
    dois = [
        item.properties.get('sci:doi') for item in items
        if item.properties.get('sci:doi') is not None
    ]
    citations = [
        item.properties.get('sci:citation') for item in items
        if item.properties.get('sci:citation') is not None
    ]
    
    if dois and len(np.unique(dois)) == 1:
        extensions.append(SCI_EXT)
        extra_fields['sci:doi'] = dois[0]
    
    if citations and len(np.unique(citations)) == 1:
        if SCI_EXT not in extensions:
            extensions.append(SCI_EXT)
        extra_fields['sci:citation'] = citations[0]
    
    # SAR metadata
    center_frequencies = [
        item.properties.get('sar:center_frequency') for item in items
        if item.properties.get('sar:center_frequency') is not None
    ]
    bandwidths = [
        item.properties.get('sar:bandwidth') for item in items
        if item.properties.get('sar:bandwidth') is not None
    ]
    
    if center_frequencies and len(np.unique(center_frequencies)) == 1:
        if SAR_EXT not in extensions:
            extensions.append(SAR_EXT)
        extra_fields['sar:center_frequency'] = center_frequencies[0]
    
    if bandwidths and len(np.unique(bandwidths)) == 1:
        if SAR_EXT not in extensions:
            extensions.append(SAR_EXT)
        extra_fields['sar:bandwidth'] = bandwidths[0]
    
    return extensions, extra_fields


def build_hierarchical_catalog(
    campaigns: List[Dict[str, Any]],
    data_root: Path,
    catalog_id: str = "OPR",
    catalog_description: str = "Open Polar Radar airborne data",
    data_product: str = "CSARP_standard",
    extra_data_products: Optional[List[str]] = None,
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_flights: Optional[int] = None,
    verbose: bool = False
) -> pystac.Catalog:
    """
    Build a hierarchical STAC catalog from campaigns.
    
    Creates a catalog structure: catalog -> campaigns -> flights -> items
    
    Parameters
    ----------
    campaigns : list of dict
        List of campaign dictionaries to process
    data_root : Path
        Root directory containing campaign data
    catalog_id : str, optional
        Catalog identifier
    catalog_description : str, optional
        Catalog description
    data_product : str, optional
        Primary data product to process
    extra_data_products : list of str, optional
        Additional data products to include
    base_url : str, optional
        Base URL for asset hrefs
    max_flights : int, optional
        Maximum number of flights per campaign
    verbose : bool, optional
        If True, print verbose output
        
    Returns
    -------
    pystac.Catalog
        Built STAC catalog with hierarchical structure
    
    Examples
    --------
    >>> campaigns = discover_campaigns(Path('/data'))
    >>> catalog = build_hierarchical_catalog(
    ...     campaigns[:2],  # Process first 2 campaigns
    ...     Path('/data'),
    ...     verbose=True
    ... )
    >>> catalog.normalize_and_save('output/catalog')
    """
    catalog = create_catalog(catalog_id=catalog_id, description=catalog_description)
    
    for campaign in campaigns:
        collection = process_single_campaign(
            campaign, data_root, data_product,
            extra_data_products, base_url, max_flights, verbose
        )
        
        if collection:
            catalog.add_child(collection)
    
    return catalog


# ============================================================================
# Export Functions
# ============================================================================

def export_to_geoparquet(catalog: pystac.Catalog, output_file: Path, verbose: bool = False) -> None:
    """
    Export catalog items to geoparquet format.
    
    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog to export
    output_file : Path
        Output parquet file path
    verbose : bool, optional
        If True, print progress messages
        
    Examples
    --------
    >>> catalog = build_hierarchical_catalog(campaigns, data_root)
    >>> export_to_geoparquet(catalog, Path('output/catalog.parquet'))
    """
    if verbose:
        print(f"Exporting to geoparquet: {output_file}")
    
    ndjson_file = output_file.with_suffix('.json')
    
    item_count = 0
    with open(ndjson_file, 'w') as f:
        for item in catalog.get_all_items():
            json.dump(item.to_dict(), f, separators=(",", ":"))
            f.write("\n")
            item_count += 1
    
    if verbose:
        print(f"  Written {item_count} items to temporary NDJSON")
    
    stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(
        str(ndjson_file), str(output_file)
    )
    
    ndjson_file.unlink()
    
    if verbose:
        print(f"  ✅ Geoparquet saved: {output_file} ({output_file.stat().st_size / 1024:.1f} KB)")


def export_collection_to_parquet(
    collection: pystac.Collection,
    output_dir: Path,
    verbose: bool = False
) -> Optional[Path]:
    """
    Export a single STAC collection to a parquet file.
    
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
    >>> collection = build_flat_collection(campaign, data_root)
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
    ndjson_file = parquet_file.with_suffix('.json')
    
    if verbose:
        print(f"  Exporting collection: {collection.id} ({len(collection_items)} items)")
    
    # Write to NDJSON
    with open(ndjson_file, 'w') as f:
        for item in collection_items:
            json.dump(item.to_dict(), f, separators=(",", ":"))
            f.write("\n")
    
    # Convert to parquet
    stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(
        str(ndjson_file), str(parquet_file)
    )
    
    # Clean up
    ndjson_file.unlink()
    
    if verbose:
        size_kb = parquet_file.stat().st_size / 1024
        print(f"  ✅ {collection.id}.parquet saved ({size_kb:.1f} KB)")
    
    return parquet_file


def export_collections_to_parquet(
    catalog: pystac.Catalog, 
    output_dir: Path, 
    verbose: bool = False
) -> Dict[str, Path]:
    """
    Export each collection to a separate parquet file.
    
    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog with collections to export
    output_dir : Path
        Output directory for parquet files
    verbose : bool, optional
        If True, print progress messages
        
    Returns
    -------
    dict
        Mapping of collection IDs to output parquet file paths
        
    Examples
    --------
    >>> catalog = build_hierarchical_catalog(campaigns, data_root)
    >>> files = export_collections_to_parquet(catalog, Path('output/'))
    >>> for collection_id, path in files.items():
    ...     print(f"{collection_id}: {path}")
    """
    output_files = {}
    
    if verbose:
        print(f"Exporting collections to separate parquet files: {output_dir}")
    
    collections = list(catalog.get_collections())
    if not collections:
        if verbose:
            print("  No collections found to export")
        return output_files
    
    for collection in collections:
        parquet_path = export_collection_to_parquet(collection, output_dir, verbose)
        if parquet_path:
            output_files[collection.id] = parquet_path
    
    return output_files


def export_collections_metadata(
    catalog: pystac.Catalog,
    output_file: Path,
    parquet_dir: Optional[Path] = None,
    verbose: bool = False
) -> None:
    """
    Export collections metadata to JSON for stac-fastapi.
    
    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog with collections
    output_file : Path
        Output JSON file path
    parquet_dir : Path, optional
        Directory containing parquet files (for relative paths)
    verbose : bool, optional
        If True, print progress messages
        
    Examples
    --------
    >>> catalog = build_hierarchical_catalog(campaigns, data_root)
    >>> export_collections_metadata(catalog, Path('output/collections.json'))
    """
    if verbose:
        print(f"Exporting collections metadata: {output_file}")
    
    collections = list(catalog.get_collections())
    if not collections:
        if verbose:
            print("  No collections found to export")
        return
    
    collections_data = []
    for collection in collections:
        collection_dict = collection.to_dict()
        
        # Build clean collection metadata
        clean_collection = {
            'type': 'Collection',
            'stac_version': collection_dict.get('stac_version', '1.1.0'),
            'id': collection.id,
            'description': collection.description or f"Collection {collection.id}",
            'license': collection_dict.get('license', 'various'),
            'extent': collection_dict.get('extent'),
            'links': []
        }
        
        # Add parquet asset reference if directory provided
        if parquet_dir:
            clean_collection['assets'] = {
                'data': {
                    'href': f"./{collection.id}.parquet",
                    'type': 'application/vnd.apache.parquet',
                    'title': 'Collection data in Apache Parquet format'
                }
            }
        
        # Add optional fields
        if 'title' in collection_dict:
            clean_collection['title'] = collection_dict['title']
        
        # Add STAC extensions if present
        if collection_dict.get('stac_extensions'):
            clean_collection['stac_extensions'] = collection_dict['stac_extensions']
        
        collections_data.append(clean_collection)
    
    # Write to JSON
    with open(output_file, 'w') as f:
        json.dump(collections_data, f, indent=2, separators=(",", ": "), default=str)
    
    if verbose:
        print(f"  ✅ Collections JSON saved: {output_file}")
        print(f"  Contains {len(collections_data)} collections")


def save_catalog(
    catalog: pystac.Catalog,
    output_dir: Path,
    catalog_type: pystac.CatalogType = pystac.CatalogType.SELF_CONTAINED
) -> None:
    """
    Save STAC catalog to disk.
    
    Parameters
    ----------
    catalog : pystac.Catalog
        Catalog to save
    output_dir : Path
        Output directory
    catalog_type : pystac.CatalogType, optional
        Type of catalog links (SELF_CONTAINED or ABSOLUTE_PUBLISHED)
        
    Examples
    --------
    >>> catalog = build_hierarchical_catalog(campaigns, data_root)
    >>> save_catalog(catalog, Path('output/'))
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_dir),
        catalog_type=catalog_type
    )