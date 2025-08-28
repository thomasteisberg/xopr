#!/usr/bin/env python3
"""
Build STAC catalog in parallel using xopr.stac.build module.

This script provides a clean interface for building STAC catalogs
from OPR data using parallel processing with Dask.
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

import pyarrow.parquet as pq
from dask.distributed import Client, as_completed

from xopr.stac import discover_campaigns, create_catalog, build_flat_catalog_dask
from xopr.stac.config import CatalogConfig
from xopr.stac.build import (
    process_single_campaign,
    save_catalog,
    export_to_geoparquet,
    export_collections_to_parquet,
    export_collections_metadata
)


class DaskClusterManager:
    """Manage Dask cluster lifecycle."""
    
    def __init__(self, n_workers: Optional[int] = None, 
                 scheduler_address: Optional[str] = None,
                 threads_per_worker: int = 2):
        """
        Initialize cluster manager.
        
        Parameters
        ----------
        n_workers : int, optional
            Number of workers to start. If None, auto-detect.
        scheduler_address : str, optional
            Address of existing scheduler to connect to.
        threads_per_worker : int, optional
            Number of threads per worker (default: 2)
        """
        self.client = None
        self.n_workers = n_workers
        self.scheduler_address = scheduler_address
        self.threads_per_worker = threads_per_worker
        
    def start(self) -> Client:
        """Start or connect to Dask cluster."""
        if self.scheduler_address:
            print(f"🔗 Connecting to existing Dask scheduler: {self.scheduler_address}")
            self.client = Client(self.scheduler_address)
        else:
            if self.n_workers is None:
                self.n_workers = max(1, min(os.cpu_count() - 1, 4))
            
            print(f"🚀 Starting local Dask cluster with {self.n_workers} workers")
            self.client = Client(
                processes=True, 
                n_workers=self.n_workers,
                threads_per_worker=self.threads_per_worker
            )
        
        self._wait_for_workers()
        return self.client
    
    def _wait_for_workers(self):
        """Wait for workers to become available."""
        max_wait = 10
        start_time = time.time()
        
        print("   Waiting for workers to start...")
        while time.time() - start_time < max_wait:
            try:
                info = self.client.scheduler_info()
                actual = len(info.get('workers', {}))
                if actual > 0:
                    break
            except Exception:
                pass
            time.sleep(0.5)
        
        # Final check
        try:
            info = self.client.scheduler_info()
            actual = len(info.get('workers', {}))
            print(f"   Dashboard: {self.client.dashboard_link}")
            print(f"   Workers running: {actual}")
            
            if actual == 0:
                print("   ⚠️  WARNING: No workers detected!")
                print("   Falling back to sequential processing...")
        except Exception as e:
            print(f"   ⚠️  Warning: Could not get scheduler info: {e}")
    
    def close(self):
        """Close the client."""
        if self.client:
            print("🔚 Shutting down Dask client...")
            self.client.close()
            self.client = None


def process_campaigns_parallel(
    client: Client,
    campaigns: List[Dict[str, Any]],
    data_root: Path,
    data_product: str = "CSARP_standard",
    extra_data_products: Optional[List[str]] = None,
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_flights: Optional[int] = None,
    verbose: bool = False
) -> List[Any]:
    """
    Process campaigns in parallel using Dask.
    
    Parameters
    ----------
    client : dask.distributed.Client
        Dask client for submitting tasks
    campaigns : list
        List of campaign dictionaries
    data_root : Path
        Root directory containing campaign data
    data_product : str
        Primary data product to process
    extra_data_products : list, optional
        Additional data products to include
    base_url : str
        Base URL for asset hrefs
    max_flights : int, optional
        Maximum number of flights per campaign
    verbose : bool
        If True, print verbose output
        
    Returns
    -------
    list
        List of campaign collections
    """
    if extra_data_products is None:
        extra_data_products = ['CSARP_layer']
    
    print(f"\n📤 Submitting {len(campaigns)} campaigns to workers...")
    
    # Submit all tasks
    futures = []
    for campaign in campaigns:
        future = client.submit(
            process_single_campaign,
            campaign,
            data_root,
            data_product,
            extra_data_products,
            base_url,
            max_flights,
            verbose,
            key=f"campaign-{campaign['name']}"
        )
        futures.append((campaign['name'], future))
        print(f"   → Queued: {campaign['name']}")
    
    print(f"\n⏳ Processing {len(campaigns)} campaigns in parallel...")
    print(f"   (Workers will process campaigns simultaneously)\n")
    
    # Collect results as they complete
    results = []
    completed = 0
    failed = []
    
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
                print(f"✅ [{completed}/{len(futures)}] Completed: {campaign_name}")
                results.append(result)
            else:
                print(f"⚠️  [{completed}/{len(futures)}] No data: {campaign_name}")
        except Exception as e:
            print(f"❌ [{completed}/{len(futures)}] Failed: {campaign_name} - {str(e)}")
            failed.append(campaign_name)
    
    if failed:
        print(f"\n⚠️  {len(failed)} campaigns failed processing")
    
    return results


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Build STAC catalog for Open Polar Radar data (parallel processing)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
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
        default=Path("./output"),
        help="Output directory for catalog"
    )
    
    # Catalog configuration
    parser.add_argument(
        "--catalog-id",
        default="OPR",
        help="Catalog ID"
    )
    parser.add_argument(
        "--catalog-description",
        default="Open Polar Radar airborne data",
        help="Catalog description"
    )
    parser.add_argument(
        "--data-product",
        default="CSARP_standard",
        help="Primary data product to process"
    )
    parser.add_argument(
        "--extra-products",
        nargs="*",
        default=["CSARP_layer"],
        help="Additional data products to include"
    )
    parser.add_argument(
        "--base-url",
        default="https://data.cresis.ku.edu/data/rds/",
        help="Base URL for asset hrefs"
    )
    
    # Processing options
    parser.add_argument(
        "--max-flights",
        type=int,
        default=None,
        help="Max number of flights to process per campaign"
    )
    parser.add_argument(
        "--campaigns",
        nargs="*",
        help="Specific campaigns to process (default: all)",
        default=None
    )
    parser.add_argument(
        "--campaigns-file",
        type=Path,
        help="File containing campaign names (one per line)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print verbose output"
    )
    
    # Catalog structure options
    parser.add_argument(
        "--flat-parquet",
        action="store_true",
        help="Use flattened catalog structure optimized for parquet servers (includes parquet export and auto-enables --export-collections)"
    )
    
    # Parallel processing
    parser.add_argument(
        "--n-workers",
        type=int,
        default=None,
        help="Number of Dask workers (default: auto-detect)"
    )
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=2,
        help="Number of threads per worker"
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        default="auto",
        help="Memory limit per Dask worker (e.g., '4GB', '8GB', 'auto')"
    )
    parser.add_argument(
        "--scheduler-address",
        type=str,
        default=None,
        help="Address of existing Dask scheduler"
    )
    
    # Export options
    parser.add_argument(
        "--export-collections",
        action="store_true",
        help="Export each collection to separate parquet files"
    )
    parser.add_argument(
        "--export-metadata",
        action="store_true",
        help="Export collections metadata JSON"
    )
    
    return parser.parse_args()


def load_campaign_filter(args):
    """Load campaign filter from arguments or file."""
    campaign_filter = None
    
    if args.campaigns_file and args.campaigns_file.exists():
        print(f"📁 Reading campaigns from file: {args.campaigns_file}")
        with open(args.campaigns_file, 'r') as f:
            campaign_filter = [
                line.strip() for line in f 
                if line.strip() and not line.strip().startswith('#')
            ]
        print(f"   Found {len(campaign_filter)} campaigns in file")
    elif args.campaigns:
        campaign_filter = args.campaigns
        print(f"📋 Processing specified campaigns: {', '.join(campaign_filter)}")
    
    return campaign_filter


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
    if args.extra_products:
        print(f"   Extra products: {', '.join(args.extra_products)}")
    print(f"   Base URL: {args.base_url}")
    
    # Auto-enable parquet exports for flat-parquet catalogs
    if args.flat_parquet and not args.export_collections:
        print("📦 Auto-enabling --export-collections for flat catalog structure")
        args.export_collections = True
    
    # Load campaign filter
    campaign_filter = load_campaign_filter(args)
    
    # Discover campaigns
    all_campaigns = discover_campaigns(args.data_root)
    if campaign_filter:
        campaigns = [c for c in all_campaigns if c['name'] in campaign_filter]
    else:
        campaigns = all_campaigns
    
    if not campaigns:
        print("❌ No campaigns found to process")
        sys.exit(1)
    
    print(f"   Found {len(campaigns)} campaigns to process")
    if args.verbose:
        for c in campaigns:
            print(f"     - {c['name']}")
    
    # Create configuration object
    config = CatalogConfig(
        data_product=args.data_product,
        extra_data_products=args.extra_products,
        base_url=args.base_url,
        max_items=args.max_flights,
        verbose=args.verbose,
        n_workers=args.n_workers,
        memory_limit=args.memory_limit,
        threads_per_worker=args.threads_per_worker
    )
    
    try:
        if args.flat_parquet:
            # Build flat catalog structure using Dask parallel processing
            print("\n📝 Building flat STAC catalog (optimized for parquet) with Dask parallelization...")
            
            # Use Dask-enabled flat catalog builder (handles cluster setup internally)
            catalog = build_flat_catalog_dask(
                data_root=args.data_root,
                output_path=args.output_dir,
                catalog_id=args.catalog_id,
                catalog_description=args.catalog_description,
                campaign_filter=[c['name'] for c in campaigns] if campaign_filter else None,
                config=config
            )
            
            print(f"   ✅ Flat catalog built and saved: {args.output_dir}/catalog.json")
        else:
            # Initialize Dask cluster for parallel processing
            cluster_manager = DaskClusterManager(
                n_workers=args.n_workers,
                scheduler_address=args.scheduler_address,
                threads_per_worker=args.threads_per_worker
            )
            
            # Start cluster
            client = cluster_manager.start()
            
            # Process campaigns in parallel
            print("\n🚀 Processing campaigns in parallel...")
            campaign_collections = process_campaigns_parallel(
                client,
                campaigns,
                args.data_root,
                args.data_product,
                args.extra_products,
                args.base_url,
                args.max_flights,
                args.verbose
            )
            
            # Build catalog from results
            print(f"\n📦 Building catalog from {len(campaign_collections)} campaign collections...")
            catalog = create_catalog(
                catalog_id=args.catalog_id,
                description=args.catalog_description
            )
            
            for collection in campaign_collections:
                catalog.add_child(collection)
            
            # Save catalog
            print(f"\n💾 Saving catalog to {args.output_dir}")
            save_catalog(catalog, args.output_dir)
            print(f"   ✅ Catalog saved: {args.output_dir}/catalog.json")
        
        # Export to geoparquet for flat catalogs
        if args.flat_parquet:
            parquet_file = args.output_dir / f"{args.catalog_id.lower()}.parquet"
            export_to_geoparquet(catalog, parquet_file, verbose=args.verbose)
        
        # Export collections to separate parquet files if requested
        if args.export_collections:
            collection_files = export_collections_to_parquet(
                catalog, args.output_dir, verbose=args.verbose
            )
            if args.verbose and collection_files:
                print(f"   Exported {len(collection_files)} collection parquet files")
        
        # Export collections metadata if requested
        if args.export_metadata:
            metadata_file = args.output_dir / "collections.json"
            export_collections_metadata(
                catalog, metadata_file,
                parquet_dir=args.output_dir if args.export_collections else None,
                verbose=args.verbose
            )
        
        # Print summary
        print("\n📊 Catalog Statistics:")
        print("=" * 50)
        
        # Count collections and items
        collections = list(catalog.get_collections())
        
        # For flat parquet catalogs, items are in parquet files, not in memory
        if args.flat_parquet:
            # Count items from parquet files
            total_items = 0
            for parquet_file in args.output_dir.glob("*.parquet"):
                if parquet_file.name != f"{args.catalog_id.lower()}.parquet":
                    # Skip the main catalog parquet if it exists
                    try:
                        metadata = pq.read_metadata(str(parquet_file))
                        total_items += metadata.num_rows
                    except:
                        pass
        else:
            # For hierarchical catalogs, items are in the catalog structure
            total_items = sum(1 for _ in catalog.get_all_items())
        
        print(f"   Catalog structure: {'Flat (no flight collections)' if args.flat_parquet else 'Hierarchical'}")
        print(f"   Campaigns processed: {len(collections)}")
        print(f"   Total items: {total_items} {'(in parquet files)' if args.flat_parquet else ''}")
        
        # Count flights per campaign (only for hierarchical)
        if not args.flat_parquet:
            for collection in collections:
                flight_collections = list(collection.get_collections())
                if flight_collections:
                    print(f"   {collection.id}: {len(flight_collections)} flights")
        
        print("\n🎉 Complete! STAC catalog ready for use.")
        print(f"   Catalog: {args.output_dir}/catalog.json")
        if args.flat_parquet:
            print(f"   GeoParquet: {parquet_file}")
        if args.export_collections:
            print(f"   Collection parquets: {args.output_dir}/<collection_id>.parquet")
        if args.export_metadata:
            print(f"   Collections metadata: {metadata_file}")
            
    except Exception as e:
        print(f"\n❌ Error building catalog: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)
        
    finally:
        # Clean up cluster (only if we started one)
        if not args.flat_parquet and 'cluster_manager' in locals():
            cluster_manager.close()


if __name__ == "__main__":
    main()