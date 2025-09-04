#!/usr/bin/env python3
"""
Aggregate STAC catalog from existing parquet files.
Creates catalog.json from parquet collection metadata.
"""

import argparse
import sys
from pathlib import Path
from glob import glob

from omegaconf import OmegaConf

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from xopr.stac.config import load_config
from xopr.stac.build import build_catalog_from_parquet_metadata


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate STAC catalog from parquet files",
        epilog="""
Examples:
  # Using config file (reads parquet files from output.path)
  %(prog)s --config config/catalog.yaml
  
  # Override parquet location
  %(prog)s "output/2016*.parquet" --config config/catalog.yaml
  
  # Standalone (no config)
  %(prog)s "*.parquet" --id "my-catalog" --description "My data"
"""
    )
    
    parser.add_argument(
        "parquet_pattern",
        nargs="?",
        help="Glob pattern for parquet files (default: {output.path}/*.parquet)"
    )
    
    parser.add_argument("--config", "-c", help="YAML configuration file")
    parser.add_argument("--env", "-e", help="Environment (test/production)")
    parser.add_argument("--id", help="Catalog ID (overrides config)")
    parser.add_argument("--description", help="Catalog description (overrides config)")
    parser.add_argument("overrides", nargs="*", help="Config overrides")
    
    args = parser.parse_args()
    
    # Load config or create minimal one
    if args.config:
        conf = load_config(args.config, args.overrides or [], args.env)
    else:
        conf = OmegaConf.create({
            'output': {
                'path': '.',
                'catalog_id': args.id or 'OPR',
                'catalog_description': args.description or 'Open Polar Radar airborne data'
            },
            'logging': {'verbose': False}
        })
    
    # Override with command line args if provided
    if args.id:
        conf.output.catalog_id = args.id
    if args.description:
        conf.output.catalog_description = args.description
    
    # Find parquet files
    if args.parquet_pattern:
        pattern = args.parquet_pattern
    else:
        pattern = str(Path(conf.output.path) / "*.parquet")
    
    parquet_files = sorted(glob(pattern))
    
    if not parquet_files:
        print(f"No parquet files found matching: {pattern}")
        sys.exit(1)
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Build catalog
    catalog_path = Path(conf.output.path) / "catalog.json"
    
    build_catalog_from_parquet_metadata(
        parquet_paths=[Path(p) for p in parquet_files],
        output_file=catalog_path,
        catalog_id=conf.output.catalog_id,
        catalog_description=conf.output.catalog_description,
        base_url=conf.assets.get('base_url'),  # Include base_url if needed
        verbose=conf.logging.verbose
    )
    
    print(f"✅ Created {catalog_path}")


if __name__ == "__main__":
    main()