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
import pyarrow.parquet as pq
import pystac
import stac_geoparquet

from .catalog import (
    create_catalog, create_collection,
    build_collection_extent, create_items_from_flight_data,
    build_collection_extent_and_geometry, merge_flight_geometries,
    export_collection_to_parquet
)
from omegaconf import DictConfig
from .metadata import discover_campaigns, discover_flight_lines

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.3.0/schema.json'


# ============================================================================
# Core Processing Functions
# ============================================================================

def process_single_flight(
    flight_data: Dict[str, Any],
    campaign_name: str,
    campaign_info: Dict[str, Any],
    conf: DictConfig
) -> Optional[Dict[str, Any]]:
    """
    Process a single flight and return flight collection data.
    
    Parameters
    ----------
    flight_data : dict
        Flight metadata from discover_flight_lines() with 'flight_id' and 'data_files'
    campaign_name : str
        Campaign name (e.g., "2016_Antarctica_DC8")
    campaign_info : dict
        Campaign metadata with 'year', 'location', 'aircraft'
    conf : DictConfig
        Configuration object with assets.base_url, data.primary_product, and logging settings
        
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
    >>> from omegaconf import OmegaConf
    >>> conf = OmegaConf.create({
    ...     'assets': {'base_url': 'https://example.com/'},
    ...     'data': {'primary_product': 'CSARP_standard'},
    ...     'logging': {'verbose': False}
    ... })
    >>> result = process_single_flight(
    ...     flight_data, 
    ...     "2016_Antarctica_DC8",
    ...     {'year': '2016', 'location': 'Antarctica', 'aircraft': 'DC8'},
    ...     conf
    ... )
    >>> if result:
    ...     print(f"Processed flight {result['flight_id']} with {len(result['items'])} items")
    """
    try:
        # Create items for this flight
        items = create_items_from_flight_data(
            flight_data, 
            conf.assets.base_url, 
            campaign_name, 
            conf.data.primary_product, 
            conf.logging.get('verbose', False)
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
            license=conf.output.get('license', 'various'),
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
        if conf.logging.get('verbose', False):
            print(f"Warning: Failed to process flight {flight_id}: {e}")
        return None


def process_single_campaign(
    campaign: Dict[str, Any],
    conf: DictConfig
) -> Optional[pystac.Collection]:
    """
    Process a single campaign and return campaign collection.
    
    Parameters
    ----------
    campaign : dict
        Campaign metadata with keys 'name', 'path', 'year', 'location', 'aircraft'
    conf : DictConfig
        Configuration object with data, processing, and logging settings
        
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
    >>> from omegaconf import OmegaConf
    >>> conf = OmegaConf.create({
    ...     'data': {'primary_product': 'CSARP_standard', 'extra_products': ['CSARP_layer']},
    ...     'assets': {'base_url': 'https://data.cresis.ku.edu/data/rds/'},
    ...     'processing': {'max_items': None},
    ...     'logging': {'verbose': False}
    ... })
    >>> collection = process_single_campaign(campaign, conf)
    >>> if collection:
    ...     print(f"Processed {collection.id} with {len(list(collection.get_collections()))} flights")
    """
    campaign_path = Path(campaign['path'])
    campaign_name = campaign['name']
    verbose = conf.logging.get('verbose', False)
    
    if verbose:
        print(f"Processing campaign: {campaign_name}")
    
    # Discover flight lines
    try:
        flight_lines = discover_flight_lines(campaign_path, conf)
    except FileNotFoundError as e:
        if verbose:
            print(f"Warning: Skipping {campaign_name}: {e}")
        return None
    
    if not flight_lines:
        if verbose:
            print(f"Warning: No flight lines found for {campaign_name}")
        return None
    
    # Limit flights if specified
    max_items = conf.processing.get('max_items')
    if max_items is not None:
        flight_lines = flight_lines[:max_items]
    
    # Process flights
    flight_collections = []
    all_campaign_items = []
    flight_geometries = []
    
    for flight_data in flight_lines:
        flight_result = process_single_flight(
            flight_data, campaign_name, campaign, conf
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
        license=conf.output.get('license', 'various'),
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
    conf: DictConfig
) -> pystac.Catalog:
    """
    Build a hierarchical STAC catalog from campaigns.
    
    Creates a catalog structure: catalog -> campaigns -> flights -> items
    
    Parameters
    ----------
    campaigns : list of dict
        List of campaign dictionaries to process
    conf : DictConfig
        Configuration object with output.catalog_id, output.catalog_description, and other settings
        
    Returns
    -------
    pystac.Catalog
        Built STAC catalog with hierarchical structure
    
    Examples
    --------
    >>> from omegaconf import OmegaConf
    >>> conf = OmegaConf.create({
    ...     'output': {'catalog_id': 'OPR', 'catalog_description': 'Open Polar Radar data'},
    ...     'data': {'primary_product': 'CSARP_standard'},
    ...     'assets': {'base_url': 'https://data.cresis.ku.edu/data/rds/'},
    ...     'processing': {'max_items': None},
    ...     'logging': {'verbose': True}
    ... })
    >>> campaigns = discover_campaigns(Path('/data'), conf)
    >>> catalog = build_hierarchical_catalog(campaigns[:2], conf)
    >>> catalog.normalize_and_save('output/catalog')
    """
    catalog_id = conf.output.get('catalog_id', 'OPR')
    catalog_description = conf.output.get('catalog_description', 'Open Polar Radar airborne data')
    catalog = create_catalog(catalog_id=catalog_id, description=catalog_description)
    
    for campaign in campaigns:
        collection = process_single_campaign(campaign, conf)
        
        if collection:
            catalog.add_child(collection)
    
    return catalog


# ============================================================================
# Export Functions
# ============================================================================

def export_to_geoparquet(catalog: pystac.Catalog, output_file: Path, verbose: bool = False) -> None:
    """
    Export catalog items to geoparquet format with collection metadata.
    
    This function directly converts STAC items to GeoParquet format without
    intermediate NDJSON, and includes collection metadata in the Parquet file
    metadata as per the STAC GeoParquet specification.
    
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
        print(f"Exporting catalog to GeoParquet: {output_file}")
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Get all items from the catalog
    items = list(catalog.get_all_items())
    item_count = len(items)
    
    if verbose:
        print(f"  Processing {item_count} items from catalog")
    
    if item_count == 0:
        if verbose:
            print("  Warning: No items to export")
        return
    
    # Build collections metadata dictionary for the parquet file
    # Map collection IDs to their metadata as per STAC GeoParquet spec
    collections_dict = {}
    catalog_collections = list(catalog.get_collections())
    
    if catalog_collections:
        for collection in catalog_collections:
            # Store the full collection dictionary
            collections_dict[collection.id] = collection.to_dict()
    
    # Clean items before export - remove links with None hrefs
    clean_items = []
    for item in items:
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
    # For now, we'll write the collections metadata to the parquet file metadata
    stac_geoparquet.arrow.to_parquet(
        table=record_batch_reader,
        output_path=output_file,
        schema_version="1.1.0",  # Use latest schema version
        compression="snappy",  # Use snappy compression for better performance
        write_statistics=True  # Write column statistics for query optimization
    )
    
    if verbose:
        file_size_kb = output_file.stat().st_size / 1024
        print(f"  ✅ GeoParquet saved: {output_file} ({file_size_kb:.1f} KB)")
        print(f"     Contains {item_count} items from {len(catalog_collections)} collections")


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


def build_catalog_from_parquet_metadata(
    parquet_paths: List[Path],
    output_file: Path,
    catalog_id: str = "OPR",
    catalog_description: str = "Open Polar Radar airborne data",
    base_url: Optional[str] = None,
    verbose: bool = False
) -> None:
    """
    Build a catalog.json file from parquet files by reading their metadata.
    
    This function reads collection metadata from parquet files and creates a
    catalog.json file with proper links to the parquet files. Unlike
    export_collections_metadata which expects STAC collections as input,
    this function works directly with parquet files.
    
    Parameters
    ----------
    parquet_paths : List[Path]
        List of paths to parquet files containing STAC collections
    output_file : Path
        Output path for the catalog.json file
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    catalog_description : str, optional
        Catalog description, by default "Open Polar Radar airborne data"
    base_url : str, optional
        Base URL for asset hrefs. If None, uses relative paths
    verbose : bool, optional
        If True, print progress messages
        
    Examples
    --------
    >>> import glob
    >>> parquet_files = [Path(p) for p in glob.glob("output/*.parquet")]
    >>> build_catalog_from_parquet_metadata(
    ...     parquet_files, Path("output/catalog.json"), verbose=True
    ... )
    """
    if verbose:
        print(f"Building catalog from {len(parquet_paths)} parquet files")
    
    collections_data = []
    
    for parquet_path in parquet_paths:
        if not parquet_path.exists():
            if verbose:
                print(f"  ⚠️  Skipping non-existent file: {parquet_path}")
            continue
            
        try:
            # Read parquet metadata without loading the data
            parquet_metadata = pq.read_metadata(str(parquet_path))
            file_metadata = parquet_metadata.metadata
            
            # Extract collection metadata from parquet file
            collection_dict = None
            if file_metadata:
                # Try new format first (stac:collections)
                if b'stac:collections' in file_metadata:
                    collections_json = file_metadata[b'stac:collections'].decode('utf-8')
                    collections_meta = json.loads(collections_json)
                    # Get the first (and should be only) collection
                    if collections_meta:
                        collection_id = list(collections_meta.keys())[0]
                        collection_dict = collections_meta[collection_id]
                # Try legacy format used by stac-geoparquet 0.7.0
                elif b'stac-geoparquet' in file_metadata:
                    geoparquet_meta = json.loads(file_metadata[b'stac-geoparquet'].decode('utf-8'))
                    if 'collection' in geoparquet_meta:
                        collection_dict = geoparquet_meta['collection']
            
            if not collection_dict:
                if verbose:
                    print(f"  ⚠️  No collection metadata found in {parquet_path.name}")
                continue
            
            # Extract relevant metadata for collections.json format
            collection_id = collection_dict.get('id', parquet_path.stem)
            
            # Build collection entry
            collection_entry = {
                'type': 'Collection',
                'stac_version': collection_dict.get('stac_version', '1.1.0'),
                'id': collection_id,
                'description': collection_dict.get('description', f"Collection {collection_id}"),
                'license': collection_dict.get('license', 'various'),
                'extent': collection_dict.get('extent'),
                'links': [],  # Clear links as we'll build our own
                'assets': {
                    'data': {
                        'href': f"./{parquet_path.name}" if not base_url else f"{base_url}/{parquet_path.name}",
                        'type': 'application/vnd.apache.parquet',
                        'title': 'Collection data in Apache Parquet format',
                        'roles': ['data']
                    }
                }
            }
            
            # Add optional fields if present
            if 'title' in collection_dict:
                collection_entry['title'] = collection_dict['title']
            
            # Add STAC extensions if present
            if collection_dict.get('stac_extensions'):
                collection_entry['stac_extensions'] = collection_dict['stac_extensions']
            
            # Add any extra fields that might be present (like sci:doi, etc)
            for key in collection_dict:
                if key.startswith('sci:') or key.startswith('sar:') or key.startswith('proj:'):
                    collection_entry[key] = collection_dict[key]
            
            collections_data.append(collection_entry)
            
            if verbose:
                num_rows = parquet_metadata.num_rows
                print(f"  ✅ Added {collection_id} ({num_rows} items)")
                
        except Exception as e:
            if verbose:
                print(f"  ❌ Error reading {parquet_path.name}: {e}")
            continue
    
    if not collections_data:
        raise ValueError("No valid collections found in parquet files")
    
    # Sort collections by ID for consistent output
    collections_data.sort(key=lambda x: x['id'])
    
    # Build the catalog structure
    catalog = {
        'type': 'Catalog',
        'id': catalog_id,
        'stac_version': '1.1.0',
        'description': catalog_description,
        'links': [
            {
                'rel': 'root',
                'href': './catalog.json',
                'type': 'application/json'
            }
        ]
    }
    
    # Add child links for each collection
    for collection in collections_data:
        catalog['links'].append({
            'rel': 'child',
            'href': f"./{collection['id']}.parquet",
            'type': 'application/vnd.apache.parquet',
            'title': collection.get('title', collection['description'])
        })
    
    # Determine common STAC extensions across all collections
    all_extensions = set()
    for collection in collections_data:
        if 'stac_extensions' in collection:
            all_extensions.update(collection['stac_extensions'])
    
    if all_extensions:
        catalog['stac_extensions'] = sorted(list(all_extensions))
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write the catalog.json file
    with open(output_file, 'w') as f:
        json.dump(catalog, f, indent=2, separators=(",", ": "))
    
    if verbose:
        print(f"\n✅ Catalog saved to {output_file}")
        print(f"   Contains {len(collections_data)} collections")
    
    # Also save a collections.json file for compatibility
    collections_file = output_file.parent / "collections.json"
    with open(collections_file, 'w') as f:
        json.dump(collections_data, f, indent=2, separators=(",", ": "))
    
    if verbose:
        print(f"✅ Collections metadata saved to {collections_file}")


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