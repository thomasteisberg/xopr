#!/usr/bin/env python3
"""
Build STAC catalog for Open Polar Radar data.

This script creates a complete STAC catalog from OPR data, prints the structure,
and exports to geoparquet format for efficient querying.
"""

import argparse
import json
from pathlib import Path
import sys

import pystac
import stac_geoparquet
from xopr.stac import (
    build_catalog_from_data_root, create_catalog, create_collection, 
    build_collection_extent, create_item_from_flight_data,
    discover_campaigns, discover_flight_lines
)


def print_catalog_structure(catalog: pystac.Catalog, indent: int = 0) -> None:
    """
    Print a hierarchical view of the catalog structure.
    
    Args:
        catalog: STAC catalog to print
        indent: Current indentation level
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
            
            # Count items in collection
            items = list(collection.get_items())
            print(f"{prefix}        Items: {len(items)}")
            
            # Print extent info
            if collection.extent.spatial.bboxes:
                bbox = collection.extent.spatial.bboxes[0]
                print(f"{prefix}        Spatial extent: [{bbox[0]:.2f}, {bbox[1]:.2f}, {bbox[2]:.2f}, {bbox[3]:.2f}]")
            
            if collection.extent.temporal.intervals:
                interval = collection.extent.temporal.intervals[0]
                if interval[0] and interval[1]:
                    print(f"{prefix}        Temporal extent: {interval[0].date()} to {interval[1].date()}")
    
    # Print child catalogs recursively
    child_catalogs = [child for child in catalog.get_children() if isinstance(child, pystac.Catalog)]
    for child in child_catalogs:
        print_catalog_structure(child, indent + 1)


def build_limited_catalog(
    data_root: Path,
    output_path: Path,
    catalog_id: str = "OPR",
    data_product: str = "CSARP_standard",
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_items: int = 10,
    campaign_filter: list = None
) -> pystac.Catalog:
    """
    Build STAC catalog with limits for faster processing.
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
            flight_lines = discover_flight_lines(campaign_path, data_product)
        except FileNotFoundError as e:
            print(f"Warning: Skipping {campaign_name}: {e}")
            continue
        
        if not flight_lines:
            print(f"Warning: No flight lines found for {campaign_name}")
            continue
        
        # Limit the number of flight lines processed if specified
        if max_items is not None:
            flight_lines = flight_lines[:max_items]
        collection_items = []
        
        for flight_data in flight_lines:
            try:
                items = create_item_from_flight_data(
                    flight_data, base_url, campaign_name, data_product
                )
                # Add all items from flight (or limit if specified)
                if max_items is not None:
                    collection_items.extend(items[:1])  # Just take first item per flight when limiting
                    if len(collection_items) >= max_items:
                        break
                else:
                    collection_items.extend(items)  # Add all items when not limiting
                    
            except Exception as e:
                print(f"Warning: Failed to process flight {flight_data['flight_id']}: {e}")
                continue
        
        if collection_items:
            extent = build_collection_extent(collection_items)
            
            collection = create_collection(
                collection_id=campaign_name,
                description=f"{campaign['year']} {campaign['aircraft']} flights over {campaign['location']}",
                extent=extent
            )
            
            collection.add_items(collection_items)
            catalog.add_child(collection)
            
            print(f"Added collection {campaign_name} with {len(collection_items)} items")
    
    output_path.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )
    
    print(f"Catalog saved to {output_path}")
    return catalog


def export_to_geoparquet(catalog: pystac.Catalog, output_file: Path) -> None:
    """
    Export catalog items to geoparquet format.
    
    Args:
        catalog: STAC catalog to export
        output_file: Output parquet file path
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
    stac_geoparquet.arrow.parse_stac_ndjson_to_parquet(str(ndjson_file), str(output_file))
    
    # Clean up temporary file
    ndjson_file.unlink()
    
    print(f"   ✅ Geoparquet saved: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


def export_to_json_catalog(catalog: pystac.Catalog, output_file: Path) -> None:
    """
    Export complete catalog with all items to a single JSON file.
    
    Args:
        catalog: STAC catalog to export
        output_file: Output JSON file path
    """
    print(f"\n📄 Exporting to JSON catalog: {output_file}")
    
    # Create a complete catalog structure with all items included
    catalog_dict = catalog.to_dict()
    
    # Add all collections with their items
    collections_with_items = []
    item_count = 0
    
    for collection in catalog.get_collections():
        collection_dict = collection.to_dict()
        
        # Add all items to the collection
        items = []
        for item in collection.get_items():
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
    
    print(f"   Written {item_count} items across {len(collections_with_items)} collections")
    print(f"   ✅ JSON catalog saved: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


def main():
    parser = argparse.ArgumentParser(description="Build STAC catalog for OPR data")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("/home/thomasteisberg/Documents/opr/opr_test_dataset_1"),
        help="Root directory containing campaign data (default: test dataset)"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Maximum number of items to process per collection (default: all items)"
    )
    parser.add_argument(
        "--campaigns",
        nargs="*",
        help="Specific campaigns to process (default: all campaigns)"
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
        "--no-geoparquet",
        action="store_true",
        help="Skip geoparquet export"
    )
    parser.add_argument(
        "--no-json-catalog",
        action="store_true",
        help="Skip JSON catalog export"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.data_root.exists():
        print(f"Error: Data root directory not found: {args.data_root}")
        sys.exit(1)
    
    print(f"🚀 Building STAC catalog from: {args.data_root}")
    print(f"   Output directory: {args.output_dir}")
    print(f"   Data product: {args.data_product}")
    print(f"   Base URL: {args.base_url}")
    print()
    
    try:
        # Build the catalog with custom processing
        catalog = build_limited_catalog(
            data_root=args.data_root,
            output_path=args.output_dir,
            catalog_id=args.catalog_id,
            data_product=args.data_product,
            base_url=args.base_url,
            max_items=args.max_items,
            campaign_filter=args.campaigns
        )
        
        print(f"\n✅ Catalog built successfully!")
        print(f"   Saved to: {args.output_dir}")
        
        # Print catalog structure
        print(f"\n📋 Catalog Structure:")
        print("=" * 50)
        print_catalog_structure(catalog)
        
        # Export to geoparquet
        if not args.no_geoparquet:
            parquet_file = args.output_dir / "opr-stac.parquet"
            export_to_geoparquet(catalog, parquet_file)
        
        # Export to JSON catalog
        if not args.no_json_catalog:
            json_catalog_file = args.output_dir / "opr-stac-catalog.json"
            export_to_json_catalog(catalog, json_catalog_file)
        
        print(f"\n🎉 Complete! STAC catalog ready for use.")
        print(f"   Catalog JSON: {args.output_dir}/catalog.json")
        if not args.no_geoparquet:
            print(f"   Geoparquet: {args.output_dir}/opr-stac.parquet")
        if not args.no_json_catalog:
            print(f"   JSON Catalog: {args.output_dir}/opr-stac-catalog.json")
        
    except Exception as e:
        print(f"❌ Error building catalog: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()