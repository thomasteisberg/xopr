#!/usr/bin/env python3
"""
Build STAC catalog for Open Polar Radar data.

This script creates a complete STAC catalog from OPR data, prints the
structure, and exports to geoparquet format for efficient querying.
"""

import argparse
import json
from pathlib import Path
import sys
from typing import List, Optional

import numpy as np
import pystac
import stac_geoparquet
import dask
from dask.distributed import Client, as_completed
from dask import delayed
from xopr.stac import (
    create_catalog, create_collection,
    build_collection_extent, create_items_from_flight_data,
    discover_campaigns, discover_flight_lines, build_limited_catalog,
    build_flat_catalog
)

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.3.0/schema.json'


def print_catalog_structure(catalog: pystac.Catalog, indent: int = 0) -> None:
    """
    Print a hierarchical view of the catalog structure.

    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog to print
    indent : int, optional
        Current indentation level, by default 0
    """
    prefix = "  " * indent
    print(f"{prefix}📁 Catalog: {catalog.id}")
    print(f"{prefix}   Description: {catalog.description}")

    # Print collections
    collections = list(catalog.get_collections())
    if collections:
        print(f"{prefix}   Collections ({len(collections)}):")
        for collection in collections:
            print(f"{prefix}     📂 {collection.id}")
            print(f"{prefix}        Description: {collection.description}")

            # Count direct items in collection
            # (should be 0 for campaign collections now)
            direct_items = list(collection.get_items())
            if direct_items:
                print(f"{prefix}        Direct Items: {len(direct_items)}")

            # Print child collections (flight collections)
            child_collections = list(collection.get_collections())
            if child_collections:
                flight_count = len(child_collections)
                print(f"{prefix}        Flight Collections ({flight_count}):")
                total_items = 0
                for flight_collection in child_collections:
                    flight_items = list(flight_collection.get_items())
                    total_items += len(flight_items)
                    item_count = len(flight_items)
                    print(
                        f"{prefix}          🛩️  {flight_collection.id} "
                        f"({item_count} items)"
                    )
                print(f"{prefix}        Total Items: {total_items}")

            # Print extent info
            if collection.extent.spatial.bboxes:
                bbox = collection.extent.spatial.bboxes[0]
                extent_str = (
                    f"[{bbox[0]:.2f}, {bbox[1]:.2f}, "
                    f"{bbox[2]:.2f}, {bbox[3]:.2f}]"
                )
                print(f"{prefix}        Spatial extent: {extent_str}")

            if collection.extent.temporal.intervals:
                interval = collection.extent.temporal.intervals[0]
                if interval[0] and interval[1]:
                    start_date = interval[0].date()
                    end_date = interval[1].date()
                    print(
                        f"{prefix}        Temporal extent: "
                        f"{start_date} to {end_date}"
                    )

    # Print child catalogs recursively
    child_catalogs = [
        child for child in catalog.get_children()
        if isinstance(child, pystac.Catalog)
    ]
    for child in child_catalogs:
        print_catalog_structure(child, indent + 1)



def export_to_geoparquet(catalog: pystac.Catalog, output_file: Path) -> None:
    """
    Export catalog items to geoparquet format.

    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog to export
    output_file : Path
        Output parquet file path
    """
    print(f"\n📦 Exporting to geoparquet: {output_file}")

    # Create temporary NDJSON file
    ndjson_file = output_file.with_suffix('.json')

    # Write all items to NDJSON
    item_count = 0
    with open(ndjson_file, 'w') as f:
        for item in catalog.get_all_items():
            json.dump(item.to_dict(), f, separators=(",", ":"))
            f.write("\n")
            item_count += 1

    print(f"   Written {item_count} items to temporary NDJSON")

    # Convert to parquet
    stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(
        str(ndjson_file), str(output_file)
    )

    # Clean up temporary file
    ndjson_file.unlink()

    print(f"   ✅ Geoparquet saved: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


def export_collections_to_separate_parquet(
    catalog: pystac.Catalog, output_dir: Path
) -> None:
    """
    Export each collection to a separate geoparquet file for
    stac-fastapi-geoparquet.

    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog to export
    output_dir : Path
        Output directory for parquet files
    """
    print(
        f"\n📦 Exporting collections to separate geoparquet files: "
        f"{output_dir}"
    )

    collections = list(catalog.get_collections())
    if not collections:
        print("   No collections found to export")
        return

    for collection in collections:
        # Get both direct items and items from child collections
        collection_items = list(collection.get_items())
        
        # If no direct items, check for items in child collections
        if not collection_items:
            for child_collection in collection.get_collections():
                collection_items.extend(list(child_collection.get_items()))
        
        if not collection_items:
            print(f"   Skipping {collection.id}: no items")
            continue

        # Create output file for this collection
        parquet_file = output_dir / f"{collection.id}.parquet"
        ndjson_file = parquet_file.with_suffix('.json')

        item_count = len(collection_items)
        print(
            f"   Processing collection: {collection.id} ({item_count} items)"
        )

        # Write collection items to NDJSON
        with open(ndjson_file, 'w') as f:
            for item in collection_items:
                json.dump(item.to_dict(), f, separators=(",", ":"))
                f.write("\n")

        # Convert to parquet
        stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(
            str(ndjson_file), str(parquet_file)
        )

        # Clean up temporary file
        ndjson_file.unlink()

        size_kb = parquet_file.stat().st_size / 1024
        print(
            f"   ✅ {collection.id}.parquet saved ({size_kb:.1f} KB)"
        )

    collection_count = len(collections)
    print(
        f"   Exported {collection_count} collections to separate parquet files"
    )


def export_collections_json(
    catalog: pystac.Catalog, output_file: Path
) -> None:
    """
    Export collections metadata to collections.json for
    stac-fastapi-geoparquet.

    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog to export
    output_file : Path
        Output collections.json file path
    """
    print(f"\n📄 Exporting collections metadata: {output_file}")

    collections = list(catalog.get_collections())
    if not collections:
        print("   No collections found to export")
        return

    collections_data = []

    for collection in collections:
        # Get basic collection info
        collection_dict = collection.to_dict()

        # Keep essential fields and add required STAC fields
        clean_collection = {
            'type': 'Collection',
            'stac_version': collection_dict.get('stac_version', '1.1.0'),
            'id': collection.id,
            'description': (
                collection.description or f"Collection {collection.id}"
            ),
            'license': collection_dict.get('license', 'other'),
            'extent': collection_dict.get('extent'),
            # Empty links array as per stac-fastapi-geoparquet format
            'links': [],
            'assets': {
                'data': {
                    'href': f"./{collection.id}.parquet",
                    'type': 'application/vnd.apache.parquet'
                }
            }
        }

        # Add title if it exists
        if 'title' in collection_dict:
            clean_collection['title'] = collection_dict['title']

        collections_data.append(clean_collection)

    # Write collections.json
    with open(output_file, 'w') as f:
        json.dump(
            collections_data, f, indent=2,
            separators=(",", ": "), default=str
        )

    print(f"   ✅ Collections JSON saved: {output_file}")
    print(f"   Contains {len(collections_data)} collections")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


def export_to_json_catalog(catalog: pystac.Catalog, output_file: Path) -> None:
    """
    Export complete catalog with all items to a single JSON file.

    Parameters
    ----------
    catalog : pystac.Catalog
        STAC catalog to export
    output_file : Path
        Output JSON file path
    """
    print(f"\n📄 Exporting to JSON catalog: {output_file}")

    # Create a complete catalog structure with all items included
    catalog_dict = catalog.to_dict()

    # Add all collections with their items
    collections_with_items = []
    item_count = 0

    for collection in catalog.get_collections():
        collection_dict = collection.to_dict()

        # Add all items to the collection (both direct and from child collections)
        items = []
        for item in collection.get_items():
            items.append(item.to_dict())
            item_count += 1
        
        # If no direct items, get items from child collections
        if not items:
            for child_collection in collection.get_collections():
                for item in child_collection.get_items():
                    items.append(item.to_dict())
                    item_count += 1

        if items:
            collection_dict['items'] = items

        collections_with_items.append(collection_dict)

    # Replace links with collections containing items
    catalog_dict['collections'] = collections_with_items

    # Write the complete catalog to JSON
    with open(output_file, 'w') as f:
        json.dump(catalog_dict, f, indent=2)

    collection_count = len(collections_with_items)
    print(
        f"   Written {item_count} items across {collection_count} collections"
    )
    print(f"   ✅ JSON catalog saved: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


def setup_dask_client(args) -> Optional[Client]:
    """
    Set up Dask client for parallel processing.
    
    Parameters
    ----------
    args : argparse.Namespace
        Command line arguments containing Dask configuration
        
    Returns
    -------
    Client or None
        Dask client if parallel processing is enabled, None otherwise
    """
    if not (args.parallel or args.parallel_items or 
            args.parallel_flights or args.parallel_campaigns):
        return None
    
    if args.scheduler_address:
        print(f"🔗 Connecting to existing Dask scheduler: {args.scheduler_address}")
        client = Client(args.scheduler_address)
    else:
        n_workers = args.n_workers
        if n_workers is None:
            # Auto-detect based on CPU cores, leaving some for system
            import os
            n_workers = max(1, os.cpu_count() - 1)
        
        print(f"🚀 Starting local Dask cluster with {n_workers} workers")
        client = Client(processes=True, n_workers=n_workers, threads_per_worker=2)
    
    # Verify workers are actually running
    worker_info = client.scheduler_info()
    actual_workers = len(worker_info.get('workers', {}))
    print(f"   Dashboard: {client.dashboard_link}")
    print(f"   Workers running: {actual_workers}")
    
    if actual_workers == 0:
        print("   ⚠️  WARNING: No workers detected! Parallel processing will not work.")
    
    return client


@delayed
def create_items_from_flight_data_delayed(
    flight_data, base_url, campaign_name, primary_data_product, verbose=False
):
    """
    Delayed version of create_items_from_flight_data for parallel processing.
    """
    return create_items_from_flight_data(
        flight_data, base_url, campaign_name, primary_data_product, verbose
    )


@delayed
def process_single_flight_delayed(
    flight_data, base_url, campaign_name, primary_data_product, 
    campaign_info, verbose=False
):
    """
    Process a single flight and return flight collection data.
    
    This is a delayed function that processes one flight's data into a 
    flight collection with all its items.
    """
    from xopr.stac.catalog import (
        build_collection_extent_and_geometry, create_collection,
        SCI_EXT, SAR_EXT
    )
    
    try:
        # Create items for this flight
        items = create_items_from_flight_data(
            flight_data, base_url, campaign_name, primary_data_product, verbose
        )
        
        if not items:
            return None
        
        # Create flight collection
        flight_id = flight_data['flight_id']
        flight_extent, flight_geometry = build_collection_extent_and_geometry(items)
        
        # Collect scientific metadata from items for flight collection
        dois = [
            item.properties.get('sci:doi') for item in items
            if item.properties.get('sci:doi') is not None
        ]
        citations = [
            item.properties.get('sci:citation') for item in items
            if item.properties.get('sci:citation') is not None
        ]
        
        # Check for unique values and prepare extensions
        flight_extensions = []
        flight_extra_fields = {}
        
        if dois and len(np.unique(dois)) == 1:
            flight_extensions.append(SCI_EXT)
            flight_extra_fields['sci:doi'] = dois[0]
        
        if citations and len(np.unique(citations)) == 1:
            if SCI_EXT not in flight_extensions:
                flight_extensions.append(SCI_EXT)
            flight_extra_fields['sci:citation'] = citations[0]
        
        # Collect SAR metadata from items for flight collection
        center_frequencies = [
            item.properties.get('sar:center_frequency')
            for item in items
            if item.properties.get('sar:center_frequency') is not None
        ]
        bandwidths = [
            item.properties.get('sar:bandwidth')
            for item in items
            if item.properties.get('sar:bandwidth') is not None
        ]
        
        if center_frequencies and len(np.unique(center_frequencies)) == 1:
            flight_extensions.append(SAR_EXT)
            flight_extra_fields['sar:center_frequency'] = center_frequencies[0]
        
        if bandwidths and len(np.unique(bandwidths)) == 1:
            if SAR_EXT not in flight_extensions:
                flight_extensions.append(SAR_EXT)
            flight_extra_fields['sar:bandwidth'] = bandwidths[0]
        
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
        
        # Add scientific extra fields to flight collection
        for key, value in flight_extra_fields.items():
            flight_collection.extra_fields[key] = value
        
        # Add items to flight collection
        flight_collection.add_items(items)
        
        return {
            'collection': flight_collection,
            'items': items,
            'geometry': flight_geometry,
            'flight_id': flight_id
        }
        
    except Exception as e:
        flight_id = flight_data.get('flight_id', 'unknown')
        print(f"Warning: Failed to process flight {flight_id}: {e}")
        return None


def process_campaign(
    campaign, data_root, data_product, extra_data_products, base_url, 
    max_items, verbose, parallel_flights=False
):
    """
    Process a single campaign and return campaign collection data.
    
    This function processes one campaign's data into a campaign collection 
    with all its flight collections.
    """
    from xopr.stac.catalog import (
        build_collection_extent, create_collection, merge_flight_geometries,
        SCI_EXT, SAR_EXT
    )
    
    campaign_path = Path(campaign['path'])
    campaign_name = campaign['name']
    
    # Get worker info if running in parallel
    try:
        from dask.distributed import get_worker
        worker = get_worker()
        worker_id = worker.address.split(":")[-1]  # Get port number as simple ID
        print(f"🔧 Worker {worker_id} processing campaign: {campaign_name}")
    except (ImportError, ValueError):
        # Not running on a Dask worker (sequential mode)
        print(f"Processing campaign: {campaign_name}")
    
    try:
        flight_lines = discover_flight_lines(
            campaign_path, data_product,
            extra_data_products=extra_data_products
        )
    except FileNotFoundError as e:
        print(f"Warning: Skipping {campaign_name}: {e}")
        return None
    
    if not flight_lines:
        print(f"Warning: No flight lines found for {campaign_name}")
        return None
    
    # Limit the number of flight lines processed if specified
    if max_items is not None:
        flight_lines = flight_lines[:max_items]
    
    if parallel_flights:
        # Process flights in parallel within this campaign
        print(f"  Processing {len(flight_lines)} flights in parallel...")
        
        flight_futures = []
        for flight_data in flight_lines:
            future = process_single_flight_delayed(
                flight_data, base_url, campaign_name, data_product, 
                campaign, verbose
            )
            flight_futures.append(future)
        
        # Compute all flight futures
        flight_results = dask.compute(*flight_futures)
        
    else:
        # Process flights sequentially
        flight_results = []
        for flight_data in flight_lines:
            result = process_single_flight_delayed(
                flight_data, base_url, campaign_name, data_product, 
                campaign, verbose
            ).compute()
            flight_results.append(result)
    
    # Filter out failed flights
    flight_results = [r for r in flight_results if r is not None]
    
    if not flight_results:
        print(f"Warning: No valid flights processed for {campaign_name}")
        return None
    
    # Extract collections, items, and geometries
    flight_collections = [r['collection'] for r in flight_results]
    all_campaign_items = []
    flight_geometries = []
    
    for result in flight_results:
        all_campaign_items.extend(result['items'])
        if result['geometry'] is not None:
            flight_geometries.append(result['geometry'])
        print(f"  Added flight collection {result['flight_id']} with {len(result['items'])} items")
    
    # Create campaign collection with extent covering all flights
    campaign_extent = build_collection_extent(all_campaign_items)
    
    # Merge flight geometries into campaign-level MultiLineString
    campaign_geometry = merge_flight_geometries(flight_geometries)
    
    # Collect scientific metadata from all campaign items
    campaign_dois = [
        item.properties.get('sci:doi') for item in all_campaign_items
        if item.properties.get('sci:doi') is not None
    ]
    campaign_citations = [
        item.properties.get('sci:citation')
        for item in all_campaign_items
        if item.properties.get('sci:citation') is not None
    ]
    
    # Check for unique values and prepare extensions
    campaign_extensions = []
    campaign_extra_fields = {}
    
    if campaign_dois and len(np.unique(campaign_dois)) == 1:
        campaign_extensions.append(SCI_EXT)
        campaign_extra_fields['sci:doi'] = campaign_dois[0]
    
    if campaign_citations and len(np.unique(campaign_citations)) == 1:
        if SCI_EXT not in campaign_extensions:
            campaign_extensions.append(SCI_EXT)
        campaign_extra_fields['sci:citation'] = campaign_citations[0]
    
    # Collect SAR metadata from all campaign items
    campaign_center_frequencies = [
        item.properties.get('sar:center_frequency')
        for item in all_campaign_items
        if item.properties.get('sar:center_frequency') is not None
    ]
    campaign_bandwidths = [
        item.properties.get('sar:bandwidth')
        for item in all_campaign_items
        if item.properties.get('sar:bandwidth') is not None
    ]
    
    if campaign_center_frequencies and len(np.unique(campaign_center_frequencies)) == 1:
        campaign_extensions.append(SAR_EXT)
        campaign_extra_fields['sar:center_frequency'] = campaign_center_frequencies[0]
    
    if campaign_bandwidths and len(np.unique(campaign_bandwidths)) == 1:
        if SAR_EXT not in campaign_extensions:
            campaign_extensions.append(SAR_EXT)
        campaign_extra_fields['sar:bandwidth'] = campaign_bandwidths[0]
    
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
    
    # Add scientific extra fields to campaign collection
    for key, value in campaign_extra_fields.items():
        campaign_collection.extra_fields[key] = value
    
    # Add flight collections as children of campaign collection
    for flight_collection in flight_collections:
        campaign_collection.add_child(flight_collection)
    
    # Show completion with worker info if parallel
    try:
        from dask.distributed import get_worker
        worker = get_worker()
        worker_id = worker.address.split(":")[-1]
        print(
            f"🔧 Worker {worker_id} completed campaign {campaign_name} with "
            f"{len(flight_collections)} flight collections and "
            f"{len(all_campaign_items)} total items"
        )
    except (ImportError, ValueError):
        print(
            f"Added campaign collection {campaign_name} with "
            f"{len(flight_collections)} flight collections and "
            f"{len(all_campaign_items)} total items"
        )
    
    return campaign_collection


# Create a delayed version for backward compatibility
process_campaign_delayed = delayed(process_campaign)


def build_parallel_catalog(
    data_root: Path,
    output_path: Path,
    catalog_id: str = "OPR",
    data_product: str = "CSARP_standard",
    extra_data_products: List[str] = ['CSARP_layer', 'CSARP_qlook'],
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_items: int = 10,
    campaign_filter: List[str] = None,
    verbose: bool = False,
    parallel_flights: bool = False,
    parallel_campaigns: bool = False,
    client: Optional[Client] = None
) -> pystac.Catalog:
    """
    Build STAC catalog with parallel processing using Dask.
    
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
    parallel_flights : bool, optional
        If True, process flights within campaigns in parallel, by default False
    parallel_campaigns : bool, optional
        If True, process campaigns in parallel, by default False
        
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
    
    if parallel_campaigns:
        print(f"🚀 Processing {len(campaigns)} campaigns in parallel on Dask workers...")
        
        # Process campaigns in parallel using Dask distributed
        from dask.distributed import as_completed
        
        if client is None:
            raise ValueError("Dask client is required for parallel campaign processing but was not provided")
        
        # Submit campaign processing tasks directly to workers
        print(f"📤 Submitting {len(campaigns)} campaign tasks to {len(client.scheduler_info()['workers'])} workers...")
        futures = []
        for campaign in campaigns:
            # Use client.submit() for true parallel execution on workers
            future = client.submit(
                process_campaign,  # Use non-delayed function
                campaign, data_root, data_product, extra_data_products, 
                base_url, max_items, verbose, 
                parallel_flights=False  # Disable nested parallelism
            )
            futures.append(future)
            print(f"   → Queued: {campaign['name']}")
        
        print(f"⏳ Campaigns processing in parallel on workers...")
        print(f"   (Output may appear interleaved as workers process different campaigns simultaneously)")
        print()
        
        # Collect results as they complete
        campaign_collections = []
        completed_count = 0
        for future in as_completed(futures):
            result = future.result()
            campaign_collections.append(result)
            completed_count += 1
            if result:
                print(f"\n✅ Campaign completed ({completed_count}/{len(campaigns)}): {result.id}")
            else:
                print(f"\n⚠️  Campaign processing returned None ({completed_count}/{len(campaigns)})")
        
    else:
        # Process campaigns sequentially
        campaign_collections = []
        for campaign in campaigns:
            collection = process_campaign_delayed(
                campaign, data_root, data_product, extra_data_products, 
                base_url, max_items, verbose, parallel_flights
            ).compute()
            campaign_collections.append(collection)
    
    # Filter out failed campaigns and add to catalog
    valid_collections = [c for c in campaign_collections if c is not None]
    for collection in valid_collections:
        catalog.add_child(collection)
    
    # Save catalog
    output_path.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
    
    print(f"Catalog saved to {output_path}")
    return catalog


def main():
    parser = argparse.ArgumentParser(
        description="Build STAC catalog for OPR data"
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("/kucresis/scratch/dataproducts/public/data/rds"),
        help="Root directory containing campaign data"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Max # of items to process per collection (default: all)"
    )
    parser.add_argument(
        "--campaigns",
        nargs="*",
        help="Specific campaigns to process (space-separated names or path to text file with one campaign per line). If not specified, processes all campaigns.",
        default=None
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./scripts/output"),
        help="Output directory for catalog (default: ./scripts/output)"
    )
    parser.add_argument(
        "--catalog-id",
        default="OPR",
        help="Catalog ID (default: OPR)"
    )
    parser.add_argument(
        "--data-product",
        default="CSARP_standard",
        help="Data product to process (default: CSARP_standard)"
    )
    parser.add_argument(
        "--base-url",
        default="https://data.cresis.ku.edu/data/rds/",
        help="Base URL for asset hrefs"
    )
    parser.add_argument(
        "--combined-geoparquet",
        action="store_true",
        help="Also export combined geoparquet file (in addition to "
             "separate collection files)"
    )
    parser.add_argument(
        "--no-json-catalog",
        action="store_true",
        help="Skip JSON catalog export"
    )
    parser.add_argument(
        "--no-separate-collections",
        action="store_true",
        help="Skip separate parquet files per collection (default is to "
             "create them)"
    )
    parser.add_argument(
        "--no-collections-json",
        action="store_true",
        help="Skip collections.json metadata file (default is to "
             "create it)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print details for each STAC item being processed"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Enable parallel processing using Dask"
    )
    parser.add_argument(
        "--parallel-items",
        action="store_true",
        help="Parallelize item creation within flights"
    )
    parser.add_argument(
        "--parallel-flights",
        action="store_true",
        help="Parallelize flight processing within campaigns"
    )
    parser.add_argument(
        "--parallel-campaigns",
        action="store_true",
        help="Parallelize campaign processing"
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=None,
        help="Number of Dask workers (default: auto-detect based on CPU cores)"
    )
    parser.add_argument(
        "--scheduler-address",
        type=str,
        default=None,
        help="Address of existing Dask scheduler (e.g., 'tcp://scheduler:8786')"
    )
    parser.add_argument(
        "--flat-parquet",
        action="store_true",
        help="Use flattened catalog structure for parquet export (catalog -> campaigns -> items, no flight collections). Campaign collections have bbox-only geometry."
    )

    args = parser.parse_args()
    
    # Process campaigns argument - handle both file paths and campaign names
    campaign_filter = None
    if args.campaigns:
        # If only one argument and it's a file path, read campaigns from file
        if len(args.campaigns) == 1 and Path(args.campaigns[0]).is_file():
            campaigns_file = Path(args.campaigns[0])
            print(f"📁 Reading campaigns from file: {campaigns_file}")
            try:
                with open(campaigns_file, 'r') as f:
                    campaign_filter = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
                print(f"   Found {len(campaign_filter)} campaigns in file")
            except Exception as e:
                print(f"Error reading campaigns file {campaigns_file}: {e}")
                sys.exit(1)
        else:
            # Treat as campaign names
            campaign_filter = args.campaigns
            print(f"📋 Processing specified campaigns: {campaign_filter}")

    # Validate inputs
    if not args.data_root.exists():
        print(f"Error: Data root directory not found: {args.data_root}")
        sys.exit(1)

    print(f"🚀 Building STAC catalog from: {args.data_root}")
    print(f"   Output directory: {args.output_dir}")
    print(f"   Data product: {args.data_product}")
    print(f"   Base URL: {args.base_url}")
    
    # Check for parallel processing options
    parallel_enabled = (args.parallel or args.parallel_items or 
                       args.parallel_flights or args.parallel_campaigns)
    
    if parallel_enabled:
        print(f"   🔄 Parallel processing enabled:")
        if args.parallel or args.parallel_flights:
            print(f"     • Flight processing: parallel")
        if args.parallel or args.parallel_campaigns:
            print(f"     • Campaign processing: parallel")
    print()

    try:
        # Set up Dask client if parallel processing is enabled
        client = setup_dask_client(args)
        
        try:
            if args.flat_parquet:
                # Use flattened catalog structure (catalog -> campaigns -> items)
                catalog = build_flat_catalog(
                    data_root=args.data_root,
                    output_path=args.output_dir,
                    catalog_id=args.catalog_id,
                    data_product=args.data_product,
                    base_url=args.base_url,
                    max_items=args.max_items,
                    campaign_filter=campaign_filter,
                    verbose=args.verbose
                )
            elif parallel_enabled:
                # Use parallel catalog building (hierarchical structure)
                catalog = build_parallel_catalog(
                    data_root=args.data_root,
                    output_path=args.output_dir,
                    catalog_id=args.catalog_id,
                    data_product=args.data_product,
                    base_url=args.base_url,
                    max_items=args.max_items,
                    campaign_filter=campaign_filter,
                    verbose=args.verbose,
                    parallel_flights=(args.parallel or args.parallel_flights),
                    parallel_campaigns=(args.parallel or args.parallel_campaigns),
                    client=client  # Pass the client for parallel processing
                )
            else:
                # Use original sequential processing (hierarchical structure)
                catalog = build_limited_catalog(
                    data_root=args.data_root,
                    output_path=args.output_dir,
                    catalog_id=args.catalog_id,
                    data_product=args.data_product,
                    base_url=args.base_url,
                    max_items=args.max_items,
                    campaign_filter=campaign_filter,
                    verbose=args.verbose
                )
        finally:
            # Clean up Dask client
            if client is not None:
                print("🔚 Shutting down Dask client...")
                client.close()

        print("\n✅ Catalog built successfully!")
        print(f"   Saved to: {args.output_dir}")

        # Print catalog structure
        print("\n📋 Catalog Structure:")
        print("=" * 50)
        print_catalog_structure(catalog)

        # Export separate parquet files per collection (default,
        # stac-fastapi-geoparquet format)
        if not args.no_separate_collections:
            export_collections_to_separate_parquet(catalog, args.output_dir)

        # Export collections.json metadata (default,
        # stac-fastapi-geoparquet format)
        if not args.no_collections_json:
            collections_json_file = args.output_dir / "collections.json"
            export_collections_json(catalog, collections_json_file)

        # Export to combined geoparquet (optional, traditional format)
        if args.combined_geoparquet:
            parquet_file = args.output_dir / "opr-stac.parquet"
            export_to_geoparquet(catalog, parquet_file)

        # Export to JSON catalog
        if not args.no_json_catalog:
            json_catalog_file = args.output_dir / "opr-stac-catalog.json"
            export_to_json_catalog(catalog, json_catalog_file)

        print("\n🎉 Complete! STAC catalog ready for use.")
        print(f"   Catalog JSON: {args.output_dir}/catalog.json")
        if not args.no_separate_collections:
            print(
                f"   Collection Parquets: "
                f"{args.output_dir}/<collection_id>.parquet"
            )
        if not args.no_collections_json:
            print(
                f"   Collections JSON: {args.output_dir}/collections.json"
            )
        if args.combined_geoparquet:
            print(
                f"   Combined Geoparquet: "
                f"{args.output_dir}/opr-stac.parquet"
            )
        if not args.no_json_catalog:
            print(
                f"   JSON Catalog: "
                f"{args.output_dir}/opr-stac-catalog.json"
            )

    except Exception as e:
        print(f"❌ Error building catalog: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
