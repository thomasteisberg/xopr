"""
Metadata extraction utilities for OPR STAC catalog creation.
"""

import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Any, Union

import h5py
import numpy as np
import scipy.io
import geopandas as gpd
import shapely
from shapely.geometry import LineString, Point, box

from xopr.opr_access import OPRConnection


def get_mat_file_type(file_path: Union[str, Path]) -> str:
    """
    Figure out if a MAT file is in HDF5 format or older MATLAB format.

    Parameters
    ----------
    file_path : Union[str, Path]
        Path to the MAT file to analyze.
    
    Returns
    -------
    str
        MIME type string: 'application/x-hdf5' if HDF5 format, 
        'application/x-matlab-data' if older MATLAB format.
    """
    # Convert string to Path if necessary
    if isinstance(file_path, str):
        file_path = Path(file_path)

    try:
        f = h5py.File(file_path, 'r')
        return 'application/x-hdf5'
    except:
        f = scipy.io.loadmat(file_path, mat_dtype=True)
        return 'application/x-matlab-data'


def extract_item_metadata(mat_file_path: Union[str, Path], max_geometry_path_length: int = 1000) -> Dict[str, Any]:
    """
    Extract spatial and temporal metadata from MAT/HDF5 file.
    
    Parameters
    ----------
    mat_file_path : Union[str, Path]
        Path to MAT/HDF5 file containing GPS time and coordinate data.
    max_geometry_path_length : int, default 1000
        Maximum number of points to include in geometry. If file contains
        more points, they will be downsampled.
        
    Returns
    -------
    dict
        Dictionary containing extracted metadata with keys:
        - 'geom' : shapely.geometry.LineString
            Flight path geometry.
        - 'bbox' : shapely.geometry.box
            Bounding box of flight path.
        - 'date' : datetime.datetime
            Mean acquisition datetime.
        
    Raises
    ------
    FileNotFoundError
        If input file doesn't exist.
    KeyError
        If required coordinate or time fields are missing from input file.
    """
    # Convert string to Path if necessary
    if isinstance(mat_file_path, str):
        mat_file_path = Path(mat_file_path)
    
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
            
        # Downsample geometry if too long
        if len(latitude) > max_geometry_path_length:
            step = (len(latitude) // max_geometry_path_length) + 1
            latitude = latitude[::step]
            longitude = longitude[::step]
        
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


def discover_campaigns(data_root: Union[str, Path]) -> List[Dict[str, str]]:
    """
    Discover all campaigns in the data directory.
    
    Parameters
    ----------
    data_root : Union[str, Path]
        Root directory containing campaign subdirectories.
        
    Returns
    -------
    list of dict
        List of campaign metadata dictionaries with keys:
        
        - 'name' : str
            Campaign directory name (e.g., "2016_Antarctica_DC8").
        - 'year' : str
            Campaign year.
        - 'location' : str
            Campaign location.
        - 'aircraft' : str
            Aircraft type.
        - 'path' : str
            Full path to campaign directory.
            
    Raises
    ------
    FileNotFoundError
        If data_root directory doesn't exist.
    """
    campaign_pattern = re.compile(r'^(\d{4})_([^_]+)_([^_]+)$')
    campaigns = []
    
    # Convert string to Path if necessary
    if isinstance(data_root, str):
        data_root = Path(data_root)
    
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


def discover_data_products(campaign_path: Union[str, Path]) -> List[str]:
    """
    Discover available data products in a campaign directory.
    
    Parameters
    ----------
    campaign_path : Union[str, Path]
        Path to campaign directory.
        
    Returns
    -------
    list of str
        List of data product names (e.g., ["CSARP_standard", "CSARP_layer"]).
        Names follow the pattern "CSARP_*".
    """
    # Convert string to Path if necessary
    if isinstance(campaign_path, str):
        campaign_path = Path(campaign_path)
    
    products = []
    csarp_pattern = re.compile(r'^CSARP_\w+$')
    
    for item in campaign_path.iterdir():
        if item.is_dir() and csarp_pattern.match(item.name):
            products.append(item.name)
    
    return sorted(products)


def discover_flight_lines(campaign_path: Union[str, Path], discovery_data_product: str = "CSARP_standard", extra_data_products : list = []) -> List[Dict[str, str]]:
    """
    Discover flight lines for a specific data product within a campaign.
    
    Parameters
    ----------
    campaign_path : Union[str, Path]
        Path to campaign directory.
    discovery_data_product : str, default "CSARP_standard"
        Data product name to look for to find eligible flights.
    extra_data_products : list of str, default []
        Additional data products to link to flight lines.
        
    Returns
    -------
    list of dict
        List of flight line metadata dictionaries with keys:
        
        - 'flight_id' : str
            Flight line identifier (e.g., "20161014_03").
        - 'date' : str
            Flight date string (YYYYMMDD format).
        - 'flight_num' : str
            Flight number string.
        - 'data_files' : dict
            Dictionary mapping data product names to dictionaries of 
            {filename: filepath} for MAT files.
            
    Raises
    ------
    FileNotFoundError
        If discovery_data_product directory doesn't exist in campaign_path.
    """
    # Convert string to Path if necessary
    if isinstance(campaign_path, str):
        campaign_path = Path(campaign_path)
    
    product_path = campaign_path / discovery_data_product

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
                
                data_files = {
                    discovery_data_product: {f.name: str(f) for f in flight_dir.glob("*.mat") if not "_img" in f.name}
                }

                # Include extra data products if specified
                for extra_product in extra_data_products:
                    extra_product_path = campaign_path / extra_product / flight_dir.name
                    #print(f"Looking for extra product {extra_product} in {extra_product_path}")
                    if extra_product_path.exists():
                        data_files[extra_product] = {f.name: str(f) for f in extra_product_path.glob("*.mat")}

                if data_files:
                    flights.append({
                        'flight_id': flight_id,
                        'date': date_part,
                        'flight_num': flight_num,
                        'data_files': data_files
                    })
    
    return sorted(flights, key=lambda x: x['flight_id'])
