#!/usr/bin/env python3
"""
Build STAC collections from OPR data using YAML configuration.
Primary workflow: Build parquet collections in parallel, then aggregate with aggregate_parquet_catalog.py
"""

import argparse
import logging
import re
import sys
import time
from pathlib import Path
from typing import List, Optional

from omegaconf import DictConfig, OmegaConf
import pystac
from dask.distributed import Client, LocalCluster, as_completed

sys.path.append(str(Path(__file__).parent.parent / "src"))
from xopr.stac.config import load_config, save_config, validate_config
from xopr.stac.metadata import discover_campaigns, discover_flight_lines, collect_uniform_metadata
from xopr.stac.catalog import create_items_from_flight_data, create_collection, export_collection_to_parquet
from xopr.stac.geometry import build_collection_extent_and_geometry


def build_collection_parallel(campaign_path: Path, conf: DictConfig) -> Optional[Path]:
    """
    Build a parquet collection for a single campaign using parallel processing.
    
    Parameters
    ----------
    campaign_path : Path
        Path to campaign directory
    conf : DictConfig
        Configuration object
        
    Returns
    -------
    Path or None
        Path to created parquet file, or None if failed
    """
    campaign_name = campaign_path.name
    print(f"📡 Processing {campaign_name}")
    
    # Discover flight lines
    flight_lines = discover_flight_lines(campaign_path, conf)
    if not flight_lines:
        print(f"  ⚠️ No flights found")
        return None
    
    # Apply max_items limit
    if conf.processing.max_items:
        flight_lines = flight_lines[:conf.processing.max_items]
    
    print(f"  Found {len(flight_lines)} flights")
    
    # Setup Dask cluster
    cluster = LocalCluster(
        n_workers=conf.processing.n_workers,
        threads_per_worker=1,
        memory_limit=conf.processing.get('memory_limit', '4GB')
    )
    
    with Client(cluster) as client:
        # Submit flight processing tasks
        futures = [
            client.submit(
                create_items_from_flight_data,
                flight_data,
                conf.assets.base_url,
                campaign_name,
                conf.data.primary_product,
                False  # verbose=False for parallel
            )
            for flight_data in flight_lines
        ]
        
        # Collect results
        all_items = []
        for future in as_completed(futures):
            try:
                items = future.result()
                all_items.extend(items)
            except Exception as e:
                logging.warning(f"Failed to process flight: {e}")
    
    if not all_items:
        print(f"  ❌ No items created")
        return None
    
    # Build collection
    extent, geometry = build_collection_extent_and_geometry(all_items)
    extensions, extra_fields = collect_uniform_metadata(
        all_items, 
        ['sci:doi', 'sci:citation', 'sar:center_frequency', 'sar:bandwidth']
    )
    
    # Parse campaign name for metadata
    parts = campaign_name.split('_')
    year = parts[0] if parts else ''
    location = parts[1] if len(parts) > 1 else ''
    aircraft = parts[2] if len(parts) > 2 else ''
    
    collection = create_collection(
        collection_id=campaign_name,
        description=f"{year} {aircraft} flights over {location}",
        extent=extent,
        license=conf.output.get('license', 'various'),
        stac_extensions=extensions,
        geometry=geometry
    )
    
    for key, value in extra_fields.items():
        collection.extra_fields[key] = value
    
    for item in all_items:
        collection.add_item(item)
    
    # Export to parquet
    output_dir = Path(conf.output.path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    parquet_path = export_collection_to_parquet(
        collection, output_dir, conf.logging.verbose, conf
    )
    
    print(f"  ✅ {len(all_items)} items → {parquet_path.name}")
    return parquet_path


def process_catalog(conf: DictConfig):
    """
    Process catalog based on configuration.
    Campaigns processed sequentially, each using parallel flight processing.
    """
    # Discover campaigns
    campaigns = discover_campaigns(Path(conf.data.root), conf)
    
    # Filter by regex if specified
    if conf.data.get('campaign_filter'):
        pattern = re.compile(conf.data.campaign_filter)
        campaigns = [c for c in campaigns if pattern.match(c['name'])]
    
    if not campaigns:
        print("No campaigns found")
        return
    
    print(f"Found {len(campaigns)} campaigns")
    
    # Process each campaign
    results = []
    for campaign in campaigns:
        try:
            path = build_collection_parallel(Path(campaign['path']), conf)
            if path:
                results.append(path)
        except Exception as e:
            logging.error(f"Failed {campaign['name']}: {e}")
    
    # Save config for reproducibility
    output_path = Path(conf.output.path)
    save_config(conf, output_path / "config_used.yaml")
    
    # Summary
    print(f"\n✅ Created {len(results)} parquet files")
    print(f"Run aggregate_parquet_catalog.py to create catalog.json")


def main():
    parser = argparse.ArgumentParser(
        description="Build STAC collections from OPR data"
    )
    
    parser.add_argument("--config", "-c", required=True, help="YAML configuration file")
    parser.add_argument("--env", "-e", help="Environment (test/production)")
    parser.add_argument("overrides", nargs="*", help="Config overrides (e.g., processing.n_workers=8)")
    
    args = parser.parse_args()
    
    try:
        conf = load_config(args.config, args.overrides, args.env)
        validate_config(conf)
        
        if conf.logging.verbose:
            print(OmegaConf.to_yaml(conf))
        
        start = time.time()
        process_catalog(conf)
        print(f"Completed in {time.time() - start:.1f}s")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()