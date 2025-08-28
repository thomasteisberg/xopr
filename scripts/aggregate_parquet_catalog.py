#!/usr/bin/env python3
"""
Aggregate STAC catalog from existing parquet files.

This script creates a catalog.json and collections.json from existing parquet files
by reading their embedded STAC collection metadata. This is useful when you have
already created parquet files and need to regenerate the catalog, or when you've
added new parquet files and need to update the catalog.
"""

import argparse
import sys
import traceback
from pathlib import Path
from glob import glob
from typing import List

# Add parent directory to path to import xopr
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from xopr.stac.build import build_catalog_from_parquet_metadata


def expand_glob_patterns(patterns: List[str]) -> List[Path]:
    """
    Expand glob patterns to actual file paths.
    
    Parameters
    ----------
    patterns : List[str]
        List of glob patterns or file paths
        
    Returns
    -------
    List[Path]
        List of resolved Path objects
    """
    all_paths = []
    for pattern in patterns:
        # Check if it's a glob pattern or a direct path
        if '*' in pattern or '?' in pattern or '[' in pattern:
            # It's a glob pattern
            matched_paths = glob(pattern, recursive=True)
            if not matched_paths:
                print(f"⚠️  Warning: No files matched pattern: {pattern}")
            all_paths.extend([Path(p) for p in matched_paths])
        else:
            # It's a direct path
            path = Path(pattern)
            if path.exists():
                all_paths.append(path)
            else:
                print(f"⚠️  Warning: File not found: {pattern}")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_paths = []
    for path in all_paths:
        if path not in seen:
            seen.add(path)
            unique_paths.append(path)
    
    return unique_paths


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Aggregate STAC catalog from existing parquet files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Aggregate all parquet files in output directory
  %(prog)s "output/*.parquet" --output output/catalog.json
  
  # Aggregate specific parquet files
  %(prog)s collection1.parquet collection2.parquet --output catalog.json
  
  # Use wildcards for campaign patterns
  %(prog)s "data/2016*.parquet" "data/2017*.parquet" --output catalog.json
  
  # Recursive search for parquet files
  %(prog)s "data/**/*.parquet" --output catalog.json
"""
    )
    
    # Input arguments
    parser.add_argument(
        "parquet_files",
        nargs="+",
        help="Parquet files or glob patterns (e.g., '*.parquet', 'output/*.parquet')"
    )
    
    # Output arguments
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("catalog.json"),
        help="Output path for catalog.json file (default: catalog.json)"
    )
    
    # Catalog metadata
    parser.add_argument(
        "--catalog-id",
        default="OPR",
        help="Catalog ID (default: OPR)"
    )
    parser.add_argument(
        "--catalog-description",
        default="Open Polar Radar airborne data",
        help="Catalog description"
    )
    
    # Optional arguments
    parser.add_argument(
        "--base-url",
        help="Base URL for asset hrefs (default: relative paths)"
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print verbose output"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually creating files"
    )
    
    args = parser.parse_args()
    
    # Expand glob patterns to actual file paths
    parquet_paths = expand_glob_patterns(args.parquet_files)
    
    if not parquet_paths:
        print("❌ Error: No parquet files found matching the provided patterns")
        print("   Patterns provided:", args.parquet_files)
        sys.exit(1)
    
    # Filter to only .parquet files
    parquet_paths = [p for p in parquet_paths if p.suffix == ".parquet"]
    
    if not parquet_paths:
        print("❌ Error: No .parquet files found in the provided paths")
        sys.exit(1)
    
    # Sort paths for consistent output
    parquet_paths.sort()
    
    print(f"📊 Found {len(parquet_paths)} parquet files to aggregate")
    if args.verbose:
        for path in parquet_paths:
            print(f"   - {path}")
    
    if args.dry_run:
        print("\n🔍 Dry run mode - no files will be created")
        print(f"   Would create: {args.output}")
        print(f"   Would create: {args.output.parent / 'collections.json'}")
        print(f"   Catalog ID: {args.catalog_id}")
        print(f"   Description: {args.catalog_description}")
        if args.base_url:
            print(f"   Base URL: {args.base_url}")
        sys.exit(0)
    
    try:
        # Build the catalog from parquet metadata
        build_catalog_from_parquet_metadata(
            parquet_paths=parquet_paths,
            output_file=args.output,
            catalog_id=args.catalog_id,
            catalog_description=args.catalog_description,
            base_url=args.base_url,
            verbose=args.verbose
        )
        
        print(f"\n🎉 Successfully created catalog files:")
        print(f"   - {args.output}")
        print(f"   - {args.output.parent / 'collections.json'}")
        
    except Exception as e:
        print(f"\n❌ Error building catalog: {e}")
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()