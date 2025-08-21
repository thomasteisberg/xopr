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

import numpy as np
import pystac
import stac_geoparquet
from xopr.stac import (
    create_catalog, create_collection,
    build_collection_extent, create_items_from_flight_data,
    discover_campaigns, discover_flight_lines, build_limited_catalog
)

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.0.0/schema.json'


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
        collection_items = list(collection.get_items())
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

    collection_count = len(collections_with_items)
    print(
        f"   Written {item_count} items across {collection_count} collections"
    )
    print(f"   ✅ JSON catalog saved: {output_file}")
    print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")


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
        help="Specific campaigns to process (default: all campaigns)",
        default=['2016_Antarctica_DC8', '2017_Antarctica_P3', '2017_Antarctica_Basler', '2018_Antarctica_DC8', '2019_Antarctica_GV', '2022_Antarctica_BaslerMKB', '2023_Antarctica_BaslerMKB']
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
            campaign_filter=args.campaigns,
            verbose=args.verbose
        )

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
