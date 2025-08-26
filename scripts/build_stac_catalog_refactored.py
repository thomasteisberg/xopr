#!/usr/bin/env python3
"""
Build STAC catalog for Open Polar Radar data - Refactored version.

This script creates a complete STAC catalog from OPR data with clean
parallel processing support using Dask.
"""

import argparse
import json
import os
import sys
import traceback
from pathlib import Path
from typing import List, Optional, Dict, Any

import numpy as np
import pystac
import stac_geoparquet
from dask.distributed import Client, as_completed

# Import STAC utilities
from xopr.stac import (
    create_catalog, create_collection,
    build_collection_extent, create_items_from_flight_data,
    discover_campaigns, discover_flight_lines, 
    build_limited_catalog, build_flat_catalog
)

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.3.0/schema.json'


# ============================================================================
# Dask Cluster Management
# ============================================================================

class DaskClusterManager:
    """Centralized Dask cluster management."""
    
    def __init__(self, n_workers: Optional[int] = None, 
                 scheduler_address: Optional[str] = None):
        """
        Initialize cluster manager.
        
        Parameters
        ----------
        n_workers : int, optional
            Number of workers to start. If None, auto-detect.
        scheduler_address : str, optional
            Address of existing scheduler to connect to.
        """
        self.client = None
        self.n_workers = n_workers
        self.scheduler_address = scheduler_address
        
    def start(self) -> Client:
        """Start or connect to Dask cluster."""
        if self.scheduler_address:
            print(f"🔗 Connecting to existing Dask scheduler: {self.scheduler_address}")
            self.client = Client(self.scheduler_address)
        else:
            if self.n_workers is None:
                self.n_workers = max(1, os.cpu_count() - 1)
            
            print(f"🚀 Starting local Dask cluster with {self.n_workers} workers")
            self.client = Client(
                processes=True, 
                n_workers=self.n_workers,
                threads_per_worker=2
            )
        
        self._verify_workers()
        return self.client
    
    def _verify_workers(self):
        """Verify workers are actually running."""
        import time
        max_wait = 10  # seconds - give more time for workers to start
        start_time = time.time()
        
        print("   Waiting for workers to start...")
        while time.time() - start_time < max_wait:
            try:
                info = self.client.scheduler_info()
                actual = len(info.get('workers', {}))
                if actual > 0:
                    break
            except Exception:
                pass  # Scheduler might not be ready yet
            time.sleep(0.5)
        
        # Final check
        try:
            info = self.client.scheduler_info()
            actual = len(info.get('workers', {}))
        except Exception as e:
            print(f"   ⚠️  Warning: Could not get scheduler info: {e}")
            actual = 0
        
        print(f"   Dashboard: {self.client.dashboard_link}")
        print(f"   Workers running: {actual}")
        
        if actual == 0:
            print("   ⚠️  WARNING: No workers detected!")
            print("   Attempting to continue anyway...")
    
    def close(self):
        """Close the client."""
        if self.client:
            print("🔚 Shutting down Dask client...")
            self.client.close()
            self.client = None


# ============================================================================
# Campaign Processing Functions (for workers)
# ============================================================================

def process_single_campaign(
    campaign: Dict[str, Any],
    config: Dict[str, Any]
) -> Optional[pystac.Collection]:
    """
    Process a single campaign. This runs on a Dask worker.
    
    Parameters
    ----------
    campaign : dict
        Campaign metadata with 'name', 'path', 'year', 'location', 'aircraft'
    config : dict
        Configuration with data_root, data_product, base_url, etc.
        
    Returns
    -------
    pystac.Collection or None
        Campaign collection with flight subcollections
    """
    from xopr.stac.catalog import (
        build_collection_extent, create_collection, 
        merge_flight_geometries, build_collection_extent_and_geometry,
        SCI_EXT, SAR_EXT
    )
    
    # Extract config
    data_root = Path(config['data_root'])
    data_product = config['data_product']
    extra_data_products = config.get('extra_data_products', ['CSARP_layer'])
    base_url = config['base_url']
    max_items = config.get('max_items')
    verbose = config.get('verbose', False)
    
    campaign_path = Path(campaign['path'])
    campaign_name = campaign['name']
    
    # Worker identification (if running on Dask)
    worker_id = os.getpid()
    print(f"[PID {worker_id}] Processing campaign: {campaign_name}")
    
    # Discover flight lines
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
    
    # Limit flights if specified
    if max_items is not None:
        flight_lines = flight_lines[:max_items]
    
    # Process flights sequentially (within this worker)
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
            
            print(f"  Added flight {flight_result['flight_id']} with {len(flight_result['items'])} items")
    
    if not flight_collections:
        print(f"Warning: No valid flights processed for {campaign_name}")
        return None
    
    # Create campaign collection
    campaign_extent = build_collection_extent(all_campaign_items)
    campaign_geometry = merge_flight_geometries(flight_geometries)
    
    # Collect metadata for extensions
    campaign_extensions, campaign_extra_fields = collect_campaign_metadata(all_campaign_items)
    
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
    
    print(
        f"[PID {worker_id}] Completed campaign {campaign_name} with "
        f"{len(flight_collections)} flight collections and "
        f"{len(all_campaign_items)} total items"
    )
    
    return campaign_collection


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
        Flight metadata from discover_flight_lines()
    base_url : str
        Base URL for asset hrefs
    campaign_name : str
        Campaign name
    primary_data_product : str
        Primary data product name
    campaign_info : dict
        Campaign metadata
    verbose : bool
        If True, print verbose output
        
    Returns
    -------
    dict or None
        Dictionary with 'collection', 'items', 'geometry', 'flight_id'
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
        
        flight_id = flight_data['flight_id']
        flight_extent, flight_geometry = build_collection_extent_and_geometry(items)
        
        # Collect metadata for extensions
        flight_extensions, flight_extra_fields = collect_flight_metadata(items)
        
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
        print(f"Warning: Failed to process flight {flight_id}: {e}")
        if verbose:
            traceback.print_exc()
        return None


def collect_flight_metadata(items: List[pystac.Item]) -> tuple:
    """Collect metadata from items for flight collection extensions."""
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
        extensions.append(SAR_EXT)
        extra_fields['sar:center_frequency'] = center_frequencies[0]
    
    if bandwidths and len(np.unique(bandwidths)) == 1:
        if SAR_EXT not in extensions:
            extensions.append(SAR_EXT)
        extra_fields['sar:bandwidth'] = bandwidths[0]
    
    return extensions, extra_fields


def collect_campaign_metadata(items: List[pystac.Item]) -> tuple:
    """Collect metadata from items for campaign collection extensions."""
    # Same logic as flight metadata but for all campaign items
    return collect_flight_metadata(items)


# ============================================================================
# Parallel Processing Orchestration
# ============================================================================

class ParallelCampaignProcessor:
    """Handle parallel campaign processing cleanly."""
    
    def __init__(self, client: Client):
        """
        Initialize with Dask client.
        
        Parameters
        ----------
        client : dask.distributed.Client
            Dask client for submitting tasks
        """
        self.client = client
        
    def process_campaigns(
        self, 
        campaigns: List[Dict[str, Any]], 
        config: Dict[str, Any]
    ) -> List[pystac.Collection]:
        """
        Submit and collect campaign processing tasks.
        
        Parameters
        ----------
        campaigns : list
            List of campaign dictionaries
        config : dict
            Configuration dictionary
            
        Returns
        -------
        list
            List of campaign collections (None entries for failures)
        """
        print(f"\n📤 Submitting {len(campaigns)} campaigns to {len(self.client.scheduler_info()['workers'])} workers...")
        
        # Submit all tasks
        futures = []
        for campaign in campaigns:
            future = self.client.submit(
                process_single_campaign,
                campaign, 
                config,
                key=f"campaign-{campaign['name']}"  # Unique key for caching
            )
            futures.append((campaign['name'], future))
            print(f"   → Queued: {campaign['name']}")
        
        print(f"\n⏳ Processing campaigns in parallel...")
        print(f"   (Workers will process campaigns simultaneously)\n")
        
        # Collect results as they complete
        results = []
        completed = 0
        total = len(futures)
        
        for future in as_completed([f for _, f in futures]):
            completed += 1
            # Find which campaign this is
            campaign_name = None
            for name, f in futures:
                if f == future:
                    campaign_name = name
                    break
            
            try:
                result = future.result()
                if result:
                    print(f"✅ [{completed}/{total}] Completed: {campaign_name}")
                    results.append(result)
                else:
                    print(f"⚠️  [{completed}/{total}] No data: {campaign_name}")
            except Exception as e:
                print(f"❌ [{completed}/{total}] Failed: {campaign_name} - {str(e)}")
                
        return results


# ============================================================================
# Catalog Building Functions
# ============================================================================

def build_catalog_parallel(
    args: argparse.Namespace,
    processor: ParallelCampaignProcessor,
    campaigns: List[Dict[str, Any]]
) -> pystac.Catalog:
    """
    Build catalog using parallel processing.
    
    Parameters
    ----------
    args : argparse.Namespace
        Command line arguments
    processor : ParallelCampaignProcessor
        Parallel processor with Dask client
    campaigns : list
        List of campaigns to process
        
    Returns
    -------
    pystac.Catalog
        Built STAC catalog
    """
    catalog = create_catalog(catalog_id=args.catalog_id)
    
    # Prepare configuration
    config = {
        'data_root': str(args.data_root),
        'data_product': args.data_product,
        'extra_data_products': ['CSARP_layer'],  # Could be made configurable
        'base_url': args.base_url,
        'max_items': args.max_items,
        'verbose': args.verbose
    }
    
    # Process campaigns in parallel
    campaign_collections = processor.process_campaigns(campaigns, config)
    
    # Add collections to catalog
    for collection in campaign_collections:
        if collection:
            catalog.add_child(collection)
    
    # Save catalog
    args.output_dir.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(args.output_dir),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
    
    print(f"\n📁 Catalog saved to {args.output_dir}")
    return catalog


def build_catalog_sequential(
    args: argparse.Namespace,
    campaigns: List[Dict[str, Any]]
) -> pystac.Catalog:
    """
    Build catalog using sequential processing.
    
    Parameters
    ----------
    args : argparse.Namespace
        Command line arguments
    campaigns : list
        List of campaigns to process
        
    Returns
    -------
    pystac.Catalog
        Built STAC catalog
    """
    # Use existing build_limited_catalog for sequential processing
    return build_limited_catalog(
        data_root=args.data_root,
        output_path=args.output_dir,
        catalog_id=args.catalog_id,
        data_product=args.data_product,
        base_url=args.base_url,
        max_items=args.max_items,
        campaign_filter=[c['name'] for c in campaigns] if campaigns else None,
        verbose=args.verbose
    )


# ============================================================================
# Export Functions (kept from original)
# ============================================================================

def print_catalog_structure(catalog: pystac.Catalog, indent: int = 0) -> None:
    """Print a hierarchical view of the catalog structure."""
    prefix = "  " * indent
    print(f"{prefix}📁 Catalog: {catalog.id}")
    print(f"{prefix}   Description: {catalog.description}")

    collections = list(catalog.get_collections())
    if collections:
        print(f"{prefix}   Collections ({len(collections)}):")
        for collection in collections:
            print(f"{prefix}     📂 {collection.id}")
            print(f"{prefix}        Description: {collection.description}")

            # Count items
            direct_items = list(collection.get_items())
            if direct_items:
                print(f"{prefix}        Direct Items: {len(direct_items)}")

            # Child collections
            child_collections = list(collection.get_collections())
            if child_collections:
                flight_count = len(child_collections)
                print(f"{prefix}        Flight Collections ({flight_count}):")
                total_items = 0
                for flight_collection in child_collections:
                    flight_items = list(flight_collection.get_items())
                    total_items += len(flight_items)
                    item_count = len(flight_items)
                    print(f"{prefix}          🛩️  {flight_collection.id} ({item_count} items)")
                print(f"{prefix}        Total Items: {total_items}")

            # Extent info
            if collection.extent.spatial.bboxes:
                bbox = collection.extent.spatial.bboxes[0]
                print(f"{prefix}        Spatial extent: [{bbox[0]:.2f}, {bbox[1]:.2f}, {bbox[2]:.2f}, {bbox[3]:.2f}]")

            if collection.extent.temporal.intervals:
                interval = collection.extent.temporal.intervals[0]
                if interval[0] and interval[1]:
                    print(f"{prefix}        Temporal extent: {interval[0].date()} to {interval[1].date()}")

    # Child catalogs
    child_catalogs = [child for child in catalog.get_children() if isinstance(child, pystac.Catalog)]
    for child in child_catalogs:
        print_catalog_structure(child, indent + 1)


def export_catalog_outputs(catalog: pystac.Catalog, args: argparse.Namespace) -> None:
    """Export catalog to various formats based on arguments."""
    # Export separate parquet files per collection
    if not args.no_separate_collections:
        export_collections_to_separate_parquet(catalog, args.output_dir)

    # Export collections.json metadata
    if not args.no_collections_json:
        collections_json_file = args.output_dir / "collections.json"
        export_collections_json(catalog, collections_json_file)

    # Export to combined geoparquet
    if args.combined_geoparquet:
        parquet_file = args.output_dir / "opr-stac.parquet"
        export_to_geoparquet(catalog, parquet_file)

    # Export to JSON catalog
    if not args.no_json_catalog:
        json_catalog_file = args.output_dir / "opr-stac-catalog.json"
        export_to_json_catalog(catalog, json_catalog_file)


def export_collections_to_separate_parquet(catalog: pystac.Catalog, output_dir: Path) -> None:
    """Export each collection to a separate geoparquet file."""
    print(f"\n📦 Exporting collections to separate geoparquet files: {output_dir}")
    
    collections = list(catalog.get_collections())
    if not collections:
        print("   No collections found to export")
        return
    
    for collection in collections:
        # Get items from collection and subcollections
        collection_items = list(collection.get_items())
        if not collection_items:
            for child_collection in collection.get_collections():
                collection_items.extend(list(child_collection.get_items()))
        
        if not collection_items:
            print(f"   Skipping {collection.id}: no items")
            continue
        
        # Export to parquet
        parquet_file = output_dir / f"{collection.id}.parquet"
        ndjson_file = parquet_file.with_suffix('.json')
        
        print(f"   Processing collection: {collection.id} ({len(collection_items)} items)")
        
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
        
        size_kb = parquet_file.stat().st_size / 1024
        print(f"   ✅ {collection.id}.parquet saved ({size_kb:.1f} KB)")
    
    print(f"   Exported {len(collections)} collections to separate parquet files")


def export_collections_json(catalog: pystac.Catalog, output_file: Path) -> None:
    """Export collections metadata to collections.json."""
    print(f"\n📄 Exporting collections metadata: {output_file}")
    
    collections = list(catalog.get_collections())
    if not collections:
        print("   No collections found to export")
        return
    
    collections_data = []
    for collection in collections:
        collection_dict = collection.to_dict()
        clean_collection = {
            'type': 'Collection',
            'stac_version': collection_dict.get('stac_version', '1.1.0'),
            'id': collection.id,
            'description': collection.description or f"Collection {collection.id}",
            'license': collection_dict.get('license', 'other'),
            'extent': collection_dict.get('extent'),
            'links': [],
            'assets': {
                'data': {
                    'href': f"./{collection.id}.parquet",
                    'type': 'application/vnd.apache.parquet'
                }
            }
        }
        
        if 'title' in collection_dict:
            clean_collection['title'] = collection_dict['title']
        
        collections_data.append(clean_collection)
    
    with open(output_file, 'w') as f:
        json.dump(collections_data, f, indent=2, separators=(",", ": "), default=str)
    
    print(f"   ✅ Collections JSON saved: {output_file}")
    print(f"   Contains {len(collections_data)} collections")


def export_to_geoparquet(catalog: pystac.Catalog, output_file: Path) -> None:
    """Export catalog items to geoparquet format."""
    print(f"\n📦 Exporting to geoparquet: {output_file}")
    
    ndjson_file = output_file.with_suffix('.json')
    
    item_count = 0
    with open(ndjson_file, 'w') as f:
        for item in catalog.get_all_items():
            json.dump(item.to_dict(), f, separators=(",", ":"))
            f.write("\n")
            item_count += 1
    
    print(f"   Written {item_count} items to temporary NDJSON")
    
    stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(
        str(ndjson_file), str(output_file)
    )
    
    ndjson_file.unlink()
    
    print(f"   ✅ Geoparquet saved: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


def export_to_json_catalog(catalog: pystac.Catalog, output_file: Path) -> None:
    """Export complete catalog with all items to a single JSON file."""
    print(f"\n📄 Exporting to JSON catalog: {output_file}")
    
    catalog_dict = catalog.to_dict()
    collections_with_items = []
    item_count = 0
    
    for collection in catalog.get_collections():
        collection_dict = collection.to_dict()
        items = []
        
        for item in collection.get_items():
            items.append(item.to_dict())
            item_count += 1
        
        if not items:
            for child_collection in collection.get_collections():
                for item in child_collection.get_items():
                    items.append(item.to_dict())
                    item_count += 1
        
        if items:
            collection_dict['items'] = items
        
        collections_with_items.append(collection_dict)
    
    catalog_dict['collections'] = collections_with_items
    
    with open(output_file, 'w') as f:
        json.dump(catalog_dict, f, indent=2)
    
    print(f"   Written {item_count} items across {len(collections_with_items)} collections")
    print(f"   ✅ JSON catalog saved: {output_file}")


# ============================================================================
# Main Entry Point
# ============================================================================

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Build STAC catalog for Open Polar Radar data"
    )
    
    # Data paths
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("/kucresis/scratch/dataproducts/public/data/rds"),
        help="Root directory containing campaign data"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./scripts/output"),
        help="Output directory for catalog"
    )
    
    # Catalog configuration
    parser.add_argument(
        "--catalog-id",
        default="OPR",
        help="Catalog ID"
    )
    parser.add_argument(
        "--data-product",
        default="CSARP_standard",
        help="Data product to process"
    )
    parser.add_argument(
        "--base-url",
        default="https://data.cresis.ku.edu/data/rds/",
        help="Base URL for asset hrefs"
    )
    
    # Processing options
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Max number of flights to process per campaign"
    )
    parser.add_argument(
        "--campaigns",
        nargs="*",
        help="Specific campaigns to process",
        default=None
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose output"
    )
    
    # Parallel processing
    parser.add_argument(
        "--parallel-campaigns",
        action="store_true",
        help="Process campaigns in parallel using Dask"
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=None,
        help="Number of Dask workers (default: auto-detect)"
    )
    parser.add_argument(
        "--scheduler-address",
        type=str,
        default=None,
        help="Address of existing Dask scheduler"
    )
    
    # Export options
    parser.add_argument(
        "--flat-parquet",
        action="store_true",
        help="Use flattened catalog structure for parquet export"
    )
    parser.add_argument(
        "--no-separate-collections",
        action="store_true",
        help="Skip separate parquet files per collection"
    )
    parser.add_argument(
        "--no-collections-json",
        action="store_true",
        help="Skip collections.json metadata file"
    )
    parser.add_argument(
        "--no-json-catalog",
        action="store_true",
        help="Skip JSON catalog export"
    )
    parser.add_argument(
        "--combined-geoparquet",
        action="store_true",
        help="Export combined geoparquet file"
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Validate inputs
    if not args.data_root.exists():
        print(f"❌ Error: Data root directory not found: {args.data_root}")
        sys.exit(1)
    
    print(f"🚀 Building STAC catalog from: {args.data_root}")
    print(f"   Output directory: {args.output_dir}")
    print(f"   Data product: {args.data_product}")
    print(f"   Base URL: {args.base_url}")
    
    # Process campaign filter
    campaign_filter = None
    if args.campaigns:
        if len(args.campaigns) == 1 and Path(args.campaigns[0]).is_file():
            campaigns_file = Path(args.campaigns[0])
            print(f"📁 Reading campaigns from file: {campaigns_file}")
            try:
                with open(campaigns_file, 'r') as f:
                    campaign_filter = [line.strip() for line in f 
                                     if line.strip() and not line.strip().startswith('#')]
                print(f"   Found {len(campaign_filter)} campaigns in file")
            except Exception as e:
                print(f"❌ Error reading campaigns file: {e}")
                sys.exit(1)
        else:
            campaign_filter = args.campaigns
            print(f"📋 Processing specified campaigns: {campaign_filter}")
    
    # Discover campaigns
    campaigns = discover_campaigns(args.data_root)
    if campaign_filter:
        campaigns = [c for c in campaigns if c['name'] in campaign_filter]
    
    if not campaigns:
        print("❌ No campaigns found to process")
        sys.exit(1)
    
    print(f"   Found {len(campaigns)} campaigns to process")
    
    # Setup and run processing
    cluster_manager = None
    catalog = None
    
    try:
        if args.flat_parquet:
            # Use flattened catalog structure
            print("\n📄 Using flattened catalog structure for parquet export")
            catalog = build_flat_catalog(
                data_root=args.data_root,
                output_path=args.output_dir,
                catalog_id=args.catalog_id,
                data_product=args.data_product,
                base_url=args.base_url,
                max_items=args.max_items,
                campaign_filter=[c['name'] for c in campaigns],
                verbose=args.verbose
            )
            
        elif args.parallel_campaigns:
            # Parallel processing
            print("\n🚀 Using parallel campaign processing")
            
            # Start Dask cluster
            cluster_manager = DaskClusterManager(
                n_workers=args.n_workers,
                scheduler_address=args.scheduler_address
            )
            client = cluster_manager.start()
            
            # Process campaigns in parallel
            processor = ParallelCampaignProcessor(client)
            catalog = build_catalog_parallel(args, processor, campaigns)
            
        else:
            # Sequential processing
            print("\n📝 Using sequential processing")
            catalog = build_catalog_sequential(args, campaigns)
        
        # Print catalog structure
        print("\n📋 Catalog Structure:")
        print("=" * 50)
        print_catalog_structure(catalog)
        
        # Export outputs
        export_catalog_outputs(catalog, args)
        
        print("\n🎉 Complete! STAC catalog ready for use.")
        print(f"   Catalog JSON: {args.output_dir}/catalog.json")
        
        if not args.no_separate_collections:
            print(f"   Collection Parquets: {args.output_dir}/<collection_id>.parquet")
        if not args.no_collections_json:
            print(f"   Collections JSON: {args.output_dir}/collections.json")
        if args.combined_geoparquet:
            print(f"   Combined Geoparquet: {args.output_dir}/opr-stac.parquet")
        if not args.no_json_catalog:
            print(f"   JSON Catalog: {args.output_dir}/opr-stac-catalog.json")
            
    except Exception as e:
        print(f"\n❌ Error building catalog: {e}")
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up cluster
        if cluster_manager:
            cluster_manager.close()


if __name__ == "__main__":
    main()