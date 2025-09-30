#!/usr/bin/env python3
"""
Reorganize GCS hive structure to add hemisphere partition level.

This script moves collections from:
  gs://opr_stac/catalog/provider=<provider>/collection=<collection>/
to:
  gs://opr_stac/catalog/hemisphere=<north|south>/provider=<provider>/collection=<collection>/

Hemisphere is determined by collection name:
- "Antarctica" -> hemisphere=south
- "Greenland" -> hemisphere=north
"""

import subprocess
import re
import sys
from typing import List, Tuple, Optional
import argparse

def run_command(cmd: List[str]) -> Tuple[bool, str, str]:
    """Run a command and return success status, stdout, and stderr."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return True, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return False, e.stdout, e.stderr

def list_collections() -> List[str]:
    """List all collection directories in the current structure."""
    cmd = ["gsutil", "ls", "-d", "gs://opr_stac/catalog/provider=*/collection=*/"]
    success, stdout, stderr = run_command(cmd)

    if not success:
        print(f"Error listing collections: {stderr}")
        return []

    collections = [line.strip() for line in stdout.splitlines() if line.strip()]
    return collections

def determine_hemisphere(collection_path: str) -> Optional[str]:
    """Determine hemisphere based on collection name."""
    # Extract collection name from path
    match = re.search(r'collection=([^/]+)', collection_path)
    if not match:
        return None

    collection_name = match.group(1)

    if 'Antarctica' in collection_name:
        return 'south'
    elif 'Greenland' in collection_name:
        return 'north'
    else:
        print(f"Warning: Cannot determine hemisphere for collection: {collection_name}")
        return None

def build_new_path(old_path: str, hemisphere: str) -> str:
    """Build the new path with hemisphere partition."""
    # Extract provider and collection from old path
    provider_match = re.search(r'provider=([^/]+)', old_path)
    collection_match = re.search(r'collection=([^/]+)', old_path)

    if not provider_match or not collection_match:
        raise ValueError(f"Invalid path format: {old_path}")

    provider = provider_match.group(0)
    collection = collection_match.group(0)

    return f"gs://opr_stac/catalog/hemisphere={hemisphere}/{provider}/{collection}/"

def move_collection(old_path: str, new_path: str, dry_run: bool = True) -> bool:
    """Move a collection from old path to new path."""
    if dry_run:
        print(f"[DRY RUN] Would move:")
        print(f"  FROM: {old_path}")
        print(f"    TO: {new_path}")
        return True

    # Use gsutil mv with -m flag for parallel moves
    cmd = ["gsutil", "-m", "mv", old_path + "*", new_path]
    print(f"Moving: {old_path} -> {new_path}")

    success, stdout, stderr = run_command(cmd)

    if not success:
        print(f"Error moving {old_path}: {stderr}")
        return False

    return True

def main():
    parser = argparse.ArgumentParser(description="Reorganize GCS hive structure to add hemisphere partition")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Perform a dry run without actually moving files (default: True)")
    parser.add_argument("--execute", action="store_true",
                        help="Actually execute the moves (overrides --dry-run)")
    parser.add_argument("--verbose", action="store_true",
                        help="Print verbose output")

    args = parser.parse_args()

    # If --execute is specified, override dry_run
    dry_run = not args.execute

    if not dry_run:
        response = input("WARNING: This will move files in GCS. Are you sure? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            return

    print("Listing current collections...")
    collections = list_collections()

    if not collections:
        print("No collections found!")
        return

    print(f"Found {len(collections)} collections")

    # Categorize collections by hemisphere
    north_collections = []
    south_collections = []
    unknown_collections = []

    for collection_path in collections:
        hemisphere = determine_hemisphere(collection_path)
        if hemisphere == 'north':
            north_collections.append(collection_path)
        elif hemisphere == 'south':
            south_collections.append(collection_path)
        else:
            unknown_collections.append(collection_path)

    print(f"\nCategorization:")
    print(f"  North (Greenland): {len(north_collections)} collections")
    print(f"  South (Antarctica): {len(south_collections)} collections")
    print(f"  Unknown: {len(unknown_collections)} collections")

    if unknown_collections:
        print("\nUnknown collections:")
        for coll in unknown_collections:
            print(f"  {coll}")
        print("\nThese collections will not be moved.")

    if dry_run:
        print("\n" + "="*60)
        print("DRY RUN MODE - No files will be moved")
        print("="*60 + "\n")

    # Process north hemisphere collections
    print("\n--- Processing North Hemisphere (Greenland) Collections ---")
    for collection_path in north_collections:
        new_path = build_new_path(collection_path, 'north')
        move_collection(collection_path, new_path, dry_run)

    # Process south hemisphere collections
    print("\n--- Processing South Hemisphere (Antarctica) Collections ---")
    for collection_path in south_collections:
        new_path = build_new_path(collection_path, 'south')
        move_collection(collection_path, new_path, dry_run)

    if dry_run:
        print("\n" + "="*60)
        print("DRY RUN COMPLETE")
        print("To execute the actual moves, run with --execute flag")
        print("="*60)
    else:
        print("\n" + "="*60)
        print("REORGANIZATION COMPLETE")
        print("="*60)

        # Verify new structure
        print("\nVerifying new structure...")
        cmd = ["gsutil", "ls", "-d", "gs://opr_stac/catalog/hemisphere=*/"]
        success, stdout, stderr = run_command(cmd)
        if success:
            print("New hemisphere directories:")
            print(stdout)

if __name__ == "__main__":
    main()