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
    export_collection_to_parquet
)
from omegaconf import DictConfig
from .metadata import discover_campaigns, discover_flight_lines

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'


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
            conf,  # Pass config object
            conf.assets.base_url, 
            campaign_name, 
            conf.data.primary_product, 
            conf.logging.get('verbose', False)
        )
        
        if not items:
            return None
        
        flight_id = flight_data['flight_id']
        flight_extent = build_collection_extent(items)

        # Collect metadata for extensions
        flight_extensions, flight_extra_fields = collect_metadata_from_items(items)
        
        # Create flight collection (no geometry per user request - only item-level geometries)
        flight_collection = create_collection(
            collection_id=flight_id,
            description=(
                f"Flight {flight_id} data from {campaign_info['year']} "
                f"{campaign_info['aircraft']} over {campaign_info['location']}"
            ),
            extent=flight_extent,
            license=conf.output.get('license', 'various'),
            stac_extensions=flight_extensions if flight_extensions else None
            # geometry parameter removed - collection-level geometry not included in parquet
        )
        
        # Add extra fields
        for key, value in flight_extra_fields.items():
            flight_collection.extra_fields[key] = value
        
        # Add items to collection
        flight_collection.add_items(items)
        
        return {
            'collection': flight_collection,
            'items': items,
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

    for flight_data in flight_lines:
        flight_result = process_single_flight(
            flight_data, campaign_name, campaign, conf
        )

        if flight_result:
            flight_collections.append(flight_result['collection'])
            all_campaign_items.extend(flight_result['items'])
            
            if verbose:
                print(f"  Added flight {flight_result['flight_id']} with {len(flight_result['items'])} items")
    
    if not flight_collections:
        if verbose:
            print(f"Warning: No valid flights processed for {campaign_name}")
        return None
    
    # Create campaign collection
    campaign_extent = build_collection_extent(all_campaign_items)

    # Collect metadata for extensions
    campaign_extensions, campaign_extra_fields = collect_metadata_from_items(all_campaign_items)
    
    # Create campaign collection (no geometry per user request - only item-level geometries)
    campaign_collection = create_collection(
        collection_id=campaign_name,
        description=(
            f"{campaign['year']} {campaign['aircraft']} flights "
            f"over {campaign['location']}"
        ),
        extent=campaign_extent,
        license=conf.output.get('license', 'various'),
        stac_extensions=campaign_extensions if campaign_extensions else None
        # geometry parameter removed - collection-level geometry not included in parquet
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

    # Collect uniform metadata across items
    metadata_fields = [
        ('sci:doi', True),         # (property_key, needs_sci_extension)
        ('sci:citation', True),
        ('opr:frequency', False),
        ('opr:bandwidth', False)
    ]

    for prop_key, needs_sci_ext in metadata_fields:
        values = [item.properties.get(prop_key) for item in items
                  if item.properties.get(prop_key) is not None]
        if values and len(np.unique(values)) == 1:
            extra_fields[prop_key] = values[0]
            if needs_sci_ext and SCI_EXT not in extensions:
                extensions.append(SCI_EXT)
    
    return extensions, extra_fields

def build_catalog_from_parquet_metadata(
    parquet_paths: List[Path],
    output_file: Path,
    catalog_id: str = "OPR",
    catalog_description: str = "Open Polar Radar airborne data",
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
                        'href': f"./{parquet_path.name}",
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

