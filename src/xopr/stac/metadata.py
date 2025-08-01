"""
Metadata extraction utilities for OPR STAC catalog creation.
"""

import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Any

import h5py
import numpy as np
import scipy.io
import geopandas as gpd
import shapely
from shapely.geometry import LineString, Point, box


def extract_item_metadata(mat_file_path: Path) -> Dict[str, Any]:
    """
    Extract spatial and temporal metadata from MAT/HDF5 file.
    
    Args:
        mat_file_path: Path to MAT/HDF5 file containing GPS time and coordinate data
        
    Returns:
        Dictionary containing geometry, bounding box, and date metadata
        
    Raises:
        FileNotFoundError: If input file doesn't exist
        KeyError: If required fields are missing from input file
    """
    if not mat_file_path.exists():
        raise FileNotFoundError(f"MAT file not found: {mat_file_path}")
    
    try:
        f = h5py.File(mat_file_path, 'r')
        use_h5py = True
    except (OSError, h5py.h5f.FileOpenError):
        f = scipy.io.loadmat(mat_file_path, mat_dtype=True)
        use_h5py = False

    try:
        # Extract time data
        slow_time = np.squeeze(np.array(f['GPS_time']))
        date = dt.datetime.fromtimestamp(slow_time.mean())
        
        # Extract coordinate data
        if use_h5py:
            # For HDF5 format, coordinates are in 'Latitude' and 'Longitude' fields
            if 'Latitude' not in f or 'Longitude' not in f:
                raise KeyError(f"Required fields Latitude/Longitude not found in {mat_file_path}")
            
            latitude = np.squeeze(np.array(f['Latitude']))
            longitude = np.squeeze(np.array(f['Longitude']))
        else:
            # For older MATLAB format, try common field names
            lat_candidates = ['Latitude', 'lat', 'LAT']
            lon_candidates = ['Longitude', 'lon', 'LON']
            
            latitude = None
            longitude = None
            
            for lat_field in lat_candidates:
                if lat_field in f:
                    latitude = np.squeeze(np.array(f[lat_field]))
                    break
            
            for lon_field in lon_candidates:
                if lon_field in f:
                    longitude = np.squeeze(np.array(f[lon_field]))
                    break
            
            if latitude is None or longitude is None:
                raise KeyError(f"Required coordinate fields not found in {mat_file_path}")
        
        # Create geometry from coordinates
        geom_series = gpd.GeoSeries(map(Point, zip(longitude, latitude)))
        line = LineString(geom_series.tolist())
        bounds = shapely.bounds(line)
        boundingbox = box(bounds[0], bounds[1], bounds[2], bounds[3])
        
        return {
            'geom': line,
            'bbox': boundingbox,
            'date': date
        }
        
    finally:
        if hasattr(f, 'close'):
            f.close()


def discover_campaigns(data_root: Path) -> List[Dict[str, str]]:
    """
    Discover all campaigns in the data directory.
    
    Args:
        data_root: Root directory containing campaign subdirectories
        
    Returns:
        List of campaign metadata dictionaries with keys:
        - name: Campaign directory name (e.g., "2016_Antarctica_DC8")
        - year: Campaign year
        - location: Campaign location
        - aircraft: Aircraft type
        - path: Full path to campaign directory
    """
    campaign_pattern = re.compile(r'^(\d{4})_([^_]+)_([^_]+)$')
    campaigns = []
    
    if not data_root.exists():
        raise FileNotFoundError(f"Data root directory not found: {data_root}")
    
    for item in data_root.iterdir():
        if item.is_dir():
            match = campaign_pattern.match(item.name)
            if match:
                year, location, aircraft = match.groups()
                campaigns.append({
                    'name': item.name,
                    'year': year,
                    'location': location,
                    'aircraft': aircraft,
                    'path': str(item)
                })
    
    return sorted(campaigns, key=lambda x: (x['year'], x['name']))


def discover_data_products(campaign_path: Path) -> List[str]:
    """
    Discover available data products in a campaign directory.
    
    Args:
        campaign_path: Path to campaign directory
        
    Returns:
        List of data product names (e.g., ["CSARP_standard", "CSARP_layer"])
    """
    products = []
    csarp_pattern = re.compile(r'^CSARP_\w+$')
    
    for item in campaign_path.iterdir():
        if item.is_dir() and csarp_pattern.match(item.name):
            products.append(item.name)
    
    return sorted(products)


def discover_flight_lines(campaign_path: Path, data_product: str = "CSARP_standard") -> List[Dict[str, str]]:
    """
    Discover flight lines for a specific data product within a campaign.
    
    Args:
        campaign_path: Path to campaign directory
        data_product: Data product name (default: "CSARP_standard")
        
    Returns:
        List of flight line metadata dictionaries with keys:
        - flight_id: Flight line identifier (e.g., "20161014_03")
        - date: Flight date string
        - flight_num: Flight number string
        - mat_files: List of MAT file paths for this flight line
    """
    product_path = campaign_path / data_product
    
    if not product_path.exists():
        raise FileNotFoundError(f"Data product directory not found: {product_path}")
    
    flight_pattern = re.compile(r'^(\d{8}_\d+)$')
    flights = []
    
    for flight_dir in product_path.iterdir():
        if flight_dir.is_dir():
            match = flight_pattern.match(flight_dir.name)
            if match:
                flight_id = match.group(1)
                # Split on underscore to avoid assuming fixed lengths
                parts = flight_id.split('_')
                date_part = parts[0]
                flight_num = parts[1]
                
                mat_files = list(flight_dir.glob("*.mat"))
                
                # Only require MAT files to exist
                if mat_files:
                    flights.append({
                        'flight_id': flight_id,
                        'date': date_part,
                        'flight_num': flight_num,
                        'mat_files': [str(f) for f in mat_files]
                    })
    
    return sorted(flights, key=lambda x: x['flight_id'])