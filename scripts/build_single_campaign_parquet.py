#!/usr/bin/env python3
"""
Build a flat parquet file for a single campaign using parallel processing.

This script discovers flight lines within a campaign, uses Dask to parallelize
the creation of STAC items from flight data, and writes the results to a 
self-contained flat parquet collection file with appropriate metadata.
"""

import argparse
import sys
import time
from pathlib import Path
from typing import List, Optional

import pystac
from dask.distributed import Client, LocalCluster, as_completed

from xopr.stac import (
    discover_flight_lines, create_items_from_flight_data, 
    create_collection, export_collection_to_parquet, collect_uniform_metadata
)
from xopr.stac.geometry import build_collection_extent_and_geometry
from xopr.stac.config import CatalogConfig


def process_single_flight_dask(
    flight_data: dict,
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    campaign_name: str = "",
    primary_data_product: str = "CSARP_standard",
    verbose: bool = False
) -> List[pystac.Item]:
    """
    Wrapper for create_items_from_flight_data for Dask processing.
    
    Parameters
    ----------
    flight_data : dict
        Flight metadata from discover_flight_lines()
    base_url : str
        Base URL for constructing asset hrefs
    campaign_name : str
        Campaign name for URL construction
    primary_data_product : str
        Data product name to use as primary data source
    verbose : bool
        If True, print details for each item being processed
        
    Returns
    -------
    List[pystac.Item]
        List of STAC items for the flight
    """
    return create_items_from_flight_data(
        flight_data=flight_data,
        base_url=base_url,
        campaign_name=campaign_name,
        primary_data_product=primary_data_product,
        verbose=verbose
    )


def build_single_campaign_parquet(
    campaign_path: Path,
    output_path: Path,
    campaign_name: Optional[str] = None,
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    primary_data_product: str = "CSARP_standard",
    extra_data_products: Optional[List[str]] = None,
    n_workers: int = 4,
    verbose: bool = False
) -> Path:
    """
    Build a flat parquet file for a single campaign using parallel processing.
    
    Parameters
    ----------
    campaign_path : Path
        Path to the campaign directory
    output_path : Path
        Output path for the parquet file
    campaign_name : str, optional
        Campaign name. If None, derived from campaign_path
    base_url : str
        Base URL for constructing asset hrefs
    primary_data_product : str
        Primary data product to process
    extra_data_products : List[str], optional
        Additional data products to include
    n_workers : int
        Number of Dask workers
    verbose : bool
        Enable verbose output
        
    Returns
    -------
    Path
        Path to the created parquet file
    """
    if campaign_name is None:
        campaign_name = campaign_path.name
    
    print(f"🔍 Discovering flight lines for campaign: {campaign_name}")
    
    # Discover flight lines for the campaign
    flight_lines = discover_flight_lines(
        campaign_path=campaign_path,
        discovery_data_product=primary_data_product,
        extra_data_products=extra_data_products or []
    )
    
    if not flight_lines:
        print(f"❌ No flight lines found for campaign {campaign_name}")
        return None
    
    print(f"   Found {len(flight_lines)} flight lines")
    
    # Set up Dask cluster
    print(f"🚀 Starting Dask cluster with {n_workers} workers")
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit="4GB"
    )
    
    with Client(cluster) as client:
        print(f"   Dashboard: {client.dashboard_link}")
        
        # Submit flight processing tasks
        print(f"📡 Processing {len(flight_lines)} flights in parallel...")
        
        futures = []
        for flight_data in flight_lines:
            future = client.submit(
                process_single_flight_dask,
                flight_data=flight_data,
                base_url=base_url,
                campaign_name=campaign_name,
                primary_data_product=primary_data_product,
                verbose=False  # Reduce noise in parallel processing
            )
            futures.append(future)
        
        # Collect results
        all_items = []
        completed_count = 0
        
        for future in as_completed(futures):
            try:
                items = future.result()
                all_items.extend(items)
                completed_count += 1
                if verbose:
                    print(f"   Completed flight {completed_count}/{len(flight_lines)} "
                          f"({len(items)} items)")
            except Exception as e:
                print(f"   ⚠️ Failed to process flight: {e}")
                completed_count += 1
        
        print(f"✅ Processed {len(all_items)} total items from {completed_count} flights")
    
    if not all_items:
        print(f"❌ No items created for campaign {campaign_name}")
        return None
    
    # Build collection extent and geometry from items
    print("📐 Building collection extent and geometry...")
    extent, geometry = build_collection_extent_and_geometry(all_items)
    
    # Collect uniform metadata from items
    property_keys = ['sci:doi', 'sci:citation', 'sar:center_frequency', 'sar:bandwidth']
    extensions_needed, extra_fields = collect_uniform_metadata(all_items, property_keys)
    
    # Create collection
    print(f"📦 Creating STAC collection: {campaign_name}")
    collection = create_collection(
        collection_id=campaign_name,
        description=f"Open Polar Radar data for campaign {campaign_name}",
        extent=extent,
        license="various",
        stac_extensions=extensions_needed,
        geometry=geometry
    )
    
    # Add uniform metadata to collection
    for key, value in extra_fields.items():
        collection.extra_fields[key] = value
    
    # Add items to collection
    for item in all_items:
        collection.add_item(item)
    
    # Export to parquet
    print(f"💾 Writing parquet file: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    parquet_path = export_collection_to_parquet(
        collection=collection,
        output_dir=output_path.parent,
        verbose=verbose
    )
    
    if parquet_path != output_path:
        # Rename to desired output path if needed
        parquet_path.rename(output_path)
        parquet_path = output_path
    
    print(f"✅ Successfully created: {parquet_path}")
    print(f"   Items: {len(all_items)}")
    print(f"   File size: {parquet_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    return parquet_path


def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description="Build flat parquet file for a single campaign",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "campaign_path",
        type=Path,
        help="Path to campaign directory"
    )
    
    parser.add_argument(
        "output_path", 
        type=Path,
        help="Output parquet file path"
    )
    
    parser.add_argument(
        "--campaign-name",
        help="Campaign name (defaults to directory name)"
    )
    
    parser.add_argument(
        "--base-url",
        default="https://data.cresis.ku.edu/data/rds/",
        help="Base URL for asset hrefs"
    )
    
    parser.add_argument(
        "--primary-data-product",
        default="CSARP_standard",
        help="Primary data product to process"
    )
    
    parser.add_argument(
        "--extra-data-products",
        nargs="*",
        help="Additional data products to include"
    )
    
    parser.add_argument(
        "--n-workers",
        type=int,
        default=4,
        help="Number of Dask workers"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.campaign_path.exists():
        print(f"❌ Campaign path does not exist: {args.campaign_path}")
        sys.exit(1)
    
    if not args.campaign_path.is_dir():
        print(f"❌ Campaign path is not a directory: {args.campaign_path}")
        sys.exit(1)
    
    # Run processing
    start_time = time.time()
    
    try:
        parquet_path = build_single_campaign_parquet(
            campaign_path=args.campaign_path,
            output_path=args.output_path,
            campaign_name=args.campaign_name,
            base_url=args.base_url,
            primary_data_product=args.primary_data_product,
            extra_data_products=args.extra_data_products,
            n_workers=args.n_workers,
            verbose=args.verbose
        )
        
        elapsed = time.time() - start_time
        print(f"🎉 Completed in {elapsed:.1f}s")
        
        if parquet_path is None:
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()