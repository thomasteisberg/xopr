#!/usr/bin/env python3
"""
Build STAC catalog sequentially using xopr.stac.build module.

This script provides a clean interface for building STAC catalogs
from OPR data using sequential processing.
"""

import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq

from xopr.stac import discover_campaigns, build_flat_catalog
from xopr.stac.build import (
    build_hierarchical_catalog,
    save_catalog,
    export_to_geoparquet,
    export_collections_to_parquet,
    export_collections_metadata
)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Build STAC catalog for Open Polar Radar data (sequential processing)",
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
    
    try:
        if args.flat_parquet:
            # Build flat catalog structure
            print("\n📝 Building flat STAC catalog (optimized for parquet)...")
            catalog = build_flat_catalog(
                campaigns=campaigns,
                catalog_id=args.catalog_id,
                catalog_description=args.catalog_description,
                data_product=args.data_product,
                extra_data_products=args.extra_products,
                base_url=args.base_url,
                max_items=args.max_flights,
                verbose=args.verbose
            )
            
            # Save catalog (new function doesn't save automatically)
            print(f"\n💾 Saving catalog to {args.output_dir}")
            save_catalog(catalog, args.output_dir)
            print(f"   ✅ Flat catalog saved: {args.output_dir}/catalog.json")
        else:
            # Build hierarchical catalog
            print("\n📝 Building hierarchical catalog...")
            catalog = build_hierarchical_catalog(
                campaigns=campaigns,
                data_root=args.data_root,
                catalog_id=args.catalog_id,
                catalog_description=args.catalog_description,
                data_product=args.data_product,
                extra_data_products=args.extra_products,
                base_url=args.base_url,
                max_flights=args.max_flights,
                verbose=args.verbose
            )
            
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
        
        # Count flights per campaign (only for hierarchical catalogs)
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


if __name__ == "__main__":
    main()