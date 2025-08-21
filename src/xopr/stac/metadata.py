"""
Metadata extraction utilities for OPR STAC catalog creation.
"""

import re
import warnings
from pathlib import Path
from typing import Dict, List, Any, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import LineString, Point, box

from xopr.opr_access import OPRConnection



def extract_item_metadata(mat_file_path: Union[str, Path] = None, 
                         dataset=None) -> Dict[str, Any]:
    """
    Extract spatial and temporal metadata from MAT/HDF5 file or dataset.

    Parameters
    ----------
    mat_file_path : Union[str, Path], optional
        Path or URL to MAT/HDF5 file containing GPS time and coordinate data.
        If provided, the file will be loaded. Mutually exclusive with dataset.
    dataset : xarray.Dataset, optional
        Pre-loaded dataset containing GPS time and coordinate data.
        Mutually exclusive with mat_file_path.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing extracted metadata with keys:
        - 'geom' : shapely.geometry.LineString
            Flight path geometry.
        - 'bbox' : shapely.geometry.box
            Bounding box of flight path.
        - 'date' : datetime.datetime
            Mean acquisition datetime.
        - 'frequency' : float
            Center frequency in Hz.
        - 'bandwidth' : float
            Bandwidth in Hz.
        - 'doi' : str or None
            DOI if available.
        - 'citation' : str or None
            Citation text if available.
        - 'mimetype' : str
            MIME type of the data.

    Raises
    ------
    ValueError
        If both or neither mat_file_path and dataset are provided.
    FileNotFoundError
        If mat_file_path is provided but file doesn't exist (for local paths).
    KeyError
        If required coordinate or time fields are missing from dataset.
    """
    # Validate input parameters
    if (mat_file_path is None) == (dataset is None):
        raise ValueError("Exactly one of mat_file_path or dataset must be provided")
    
    should_close_dataset = False
    
    if mat_file_path is not None:
        # Convert string to Path if necessary for local file existence check
        if isinstance(mat_file_path, str):
            file_path = Path(mat_file_path)
        else:
            file_path = mat_file_path
            
        # Only check existence for local files (not URLs)
        if not str(mat_file_path).startswith(('http://', 'https://')):
            if not file_path.exists():
                raise FileNotFoundError(f"MAT file not found: {file_path}")

        opr = OPRConnection(cache_dir="radar_cache")
        ds = opr.load_frame_url(str(mat_file_path))
        should_close_dataset = True
    else:
        ds = dataset

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        date = pd.to_datetime(ds['slow_time'].mean().values).to_pydatetime()

    # Create geometry from coordinates
    geom_series = gpd.GeoSeries(map(Point, zip(ds['Longitude'].values,
                                               ds['Latitude'].values)))
    line = LineString(geom_series.tolist())
    # this is lazy -- proper implementation would convert the coordinates to polar stereographic first
    line = line.simplify(0.0001)
    bounds = shapely.bounds(line)
    boundingbox = box(bounds[0], bounds[1], bounds[2], bounds[3])

    # Radar params
    low_freq_array = ds.param_records['radar']['wfs']['f0']
    high_freq_array = ds.param_records['radar']['wfs']['f1']
    
    # Check for unique values and extract scalar
    unique_low_freq = np.unique(low_freq_array)
    if len(unique_low_freq) != 1:
        raise ValueError(f"Multiple low frequency values found: {unique_low_freq}")
    low_freq = float(unique_low_freq[0])
    
    unique_high_freq = np.unique(high_freq_array)
    if len(unique_high_freq) != 1:
        raise ValueError(f"Multiple high frequency values found: {unique_high_freq}")
    high_freq = float(unique_high_freq[0])
    
    bandwidth = float(np.abs(high_freq - low_freq))
    center_freq = float((low_freq + high_freq) / 2)

    # Science params
    doi = ds.attrs.get('doi', None)
    ror = ds.attrs.get('ror', None)
    cite = ds.attrs.get('funder_text', None)

    mime = ds.attrs['mimetype']

    # Only close the dataset if we opened it
    if should_close_dataset:
        ds.close()

    return {
        'geom': line,
        'bbox': boundingbox,
        'date': date,
        'frequency': center_freq,
        'bandwidth': bandwidth,
        'doi': doi,
        'citation': cite,
        'mimetype': mime
    }


def discover_campaigns(data_root: Union[str, Path]) -> List[Dict[str, str]]:
    """
    Discover all campaigns in the data directory.

    Parameters
    ----------
    data_root : Union[str, Path]
        Root directory containing campaign subdirectories.

    Returns
    -------
    List[Dict[str, str]]
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
    List[str]
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


def discover_flight_lines(campaign_path: Union[str, Path],
                          discovery_data_product: str = "CSARP_standard",
                          extra_data_products: list = None) -> List[Dict[str, str]]:
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
    List[Dict[str, str]]
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
                    discovery_data_product: {
                        f.name: str(f) for f in flight_dir.glob("*.mat")
                        if "_img" not in f.name
                    }
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
