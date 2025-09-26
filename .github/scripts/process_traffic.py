#!/usr/bin/env python3
"""
Process GitHub traffic data and update Parquet storage.
This script fetches today's traffic data and appends it to the historical Parquet file.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def load_json_file(filepath):
    """Load JSON file safely."""
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load {filepath}: {e}")
        return None


def process_traffic_data(raw_dir='/tmp/traffic-data/raw', data_dir='/tmp/traffic-data'):
    """Process today's traffic data and update Parquet file."""
    raw_dir = Path(raw_dir)
    data_dir = Path(data_dir)
    
    # Get today's date
    today = datetime.now().strftime('%Y-%m-%d')
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    print(f"📅 Processing data for {today}")
    
    # Load today's data
    clones = load_json_file(raw_dir / f'clones-{today}.json')
    views = load_json_file(raw_dir / f'views-{today}.json')
    paths = load_json_file(raw_dir / f'paths-{today}.json')
    referrers = load_json_file(raw_dir / f'referrers-{today}.json')
    
    # Create today's record
    today_record = {
        'date': pd.to_datetime(today),
        'timestamp': pd.to_datetime(timestamp),
        'clones_total': clones.get('count', 0) if clones else 0,
        'clones_unique': clones.get('uniques', 0) if clones else 0,
        'views_total': views.get('count', 0) if views else 0,
        'views_unique': views.get('uniques', 0) if views else 0,
        # Store top paths and referrers as JSON strings
        'top_paths': json.dumps(paths['paths'][:5] if paths and 'paths' in paths else []),
        'top_referrers': json.dumps(referrers['referrers'][:5] if referrers and 'referrers' in referrers else [])
    }
    
    print(f"📊 Today's data ({today}):")
    print(f"  Clones: {today_record['clones_total']} total, {today_record['clones_unique']} unique")
    print(f"  Views: {today_record['views_total']} total, {today_record['views_unique']} unique")
    
    # Load existing parquet or create new
    parquet_file = data_dir / 'traffic_history.parquet'
    if parquet_file.exists():
        print("📂 Loading existing Parquet file...")
        df = pd.read_parquet(parquet_file)
        
        # Check if today's data already exists and update/append
        mask = df['date'] == pd.to_datetime(today)
        if mask.any():
            print(f"  Updating existing entry for {today}")
            # Update the row
            for col, val in today_record.items():
                df.loc[mask, col] = val
        else:
            print(f"  Appending new entry for {today}")
            df = pd.concat([df, pd.DataFrame([today_record])], ignore_index=True)
    else:
        print("📄 Creating new Parquet file...")
        df = pd.DataFrame([today_record])
    
    # Sort by date
    df = df.sort_values('date').reset_index(drop=True)
    
    # Save as Parquet with compression
    df.to_parquet(parquet_file, compression='snappy', index=False)
    print(f"💾 Saved {len(df)} records to Parquet")
    
    # Create/update metadata
    metadata = {
        'last_updated': timestamp,
        'date_range': {
            'start': df['date'].min().isoformat(),
            'end': df['date'].max().isoformat()
        },
        'record_count': len(df),
        'schema_version': '1.0',
        'columns': list(df.columns),
        'file_size_bytes': parquet_file.stat().st_size,
        'parquet_file': 'traffic_history.parquet',
        'collection_stats': {
            'total_clones': int(df['clones_total'].sum()),
            'total_views': int(df['views_total'].sum()),
            'unique_clones': int(df['clones_unique'].sum()),
            'unique_views': int(df['views_unique'].sum()),
            'days_collected': len(df),
            'avg_daily_clones': float(df['clones_total'].mean()),
            'avg_daily_views': float(df['views_total'].mean())
        }
    }
    
    # Check for missing dates
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max())
    missing_dates = date_range.difference(df['date']).strftime('%Y-%m-%d').tolist()
    if missing_dates:
        metadata['collection_stats']['missing_dates'] = missing_dates
        print(f"⚠️  Found {len(missing_dates)} missing dates in collection")
    
    # Save metadata
    metadata_file = data_dir / 'metadata.json'
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"📋 Metadata updated: {metadata['record_count']} total records")
    print(f"   File size: {metadata['file_size_bytes'] / 1024:.1f} KB")
    
    return df, metadata


if __name__ == '__main__':
    # Allow command line args for directories
    raw_dir = sys.argv[1] if len(sys.argv) > 1 else '/tmp/traffic-data/raw'
    data_dir = sys.argv[2] if len(sys.argv) > 2 else '/tmp/traffic-data'
    
    try:
        df, metadata = process_traffic_data(raw_dir, data_dir)
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error processing traffic data: {e}")
        sys.exit(1)