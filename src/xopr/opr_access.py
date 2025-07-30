from typing import Iterable
import xarray as xr
import fsspec
import pandas as pd
import numpy as np
import requests
import json

from xopr.cf_units import apply_cf_compliant_attrs
import xopr.ops_api

class OPRConnection:
    def __init__(self, collection_url: str = "https://data.cresis.ku.edu/data/", cache_dir: str = None):
        """
        Initialize the OPRConnection with a collection URL and optional cache directory.

        Parameters
        ----------
        collection_url : str
            The base URL for the OPR data collection.
        cache_dir : str, optional
            Directory to cache downloaded data.
        """
        self.collection_url = collection_url
        self.cache_dir = cache_dir

        self.fsspec_cache_kwargs = {}
        self.fsspec_url_prefix = ''
        if cache_dir:
            self.fsspec_cache_kwargs = {
                'cache_storage': cache_dir,
                'check_files': True
            }
            self.fsspec_url_prefix = 'filecache::'


    def load_frame(self, url: str) -> xr.Dataset:
        """
        Load a radar frame from a given URL.

        Parameters
        ----------
        url : str
            The URL of the radar frame data.

        Returns
        -------
        xr.Dataset
            The loaded radar frame as an xarray Dataset.
        """
        
        file = fsspec.open_local(f"{self.fsspec_url_prefix}{url}", filecache=self.fsspec_cache_kwargs)
        ds = xr.open_dataset(file, engine='h5netcdf', phony_dims='sort')

        # Re-arrange variables to provide useful dimensions and coordinates

        ds = ds.squeeze() # Drop the singleton dimensions matlab adds

        ds = ds.rename({ # Label the dimensions with more useful names
            ds.Data.dims[0]: 'slow_time_idx',
            ds.Data.dims[1]: 'twtt_idx',
        })

        # Make variables with no dimensions into scalar attributes
        for var in ds.data_vars:
            if ds[var].ndim == 0:
                ds.attrs[var] = ds[var].item()
                ds = ds.drop_vars(var)
        
        # Make the file_type an attribute
        if 'file_type' in ds.data_vars:
            ds.attrs['file_type'] = ds['file_type'].to_numpy()
            ds = ds.drop_vars('file_type')

        # Name the two time coordinates
        ds = ds.rename({'Time': 'twtt', 'GPS_time': 'slow_time'})
        ds = ds.set_coords(['slow_time', 'twtt'])

        slow_time_1d = pd.to_datetime(ds['slow_time'].values, unit='s')
        ds = ds.assign_coords(slow_time=('slow_time_idx', slow_time_1d))

        # Make twtt and slow_time the indexing coordinates
        ds = ds.swap_dims({'twtt_idx': 'twtt'})
        ds = ds.swap_dims({'slow_time_idx': 'slow_time'})


        # Apply CF-compliant attributes
        ds = apply_cf_compliant_attrs(ds)

        # Get the season and segment from the URL
        import re
        match = re.search(r'(\d{4}_\w+_[A-Za-z0-9]+)\/[\w_]+\/([\d_]+)', url)
        if match:
            season, segment = match.groups()
            ds.attrs['season'] = season
            ds.attrs['segment'] = segment

            # Load citation information
            result = xopr.ops_api.get_segment_metadata(segment_name=segment, season_name=season)
            if result:
                result_data = {}
                for key, value in result['data'].items():
                    if len(value) == 1:
                        result_data[key] = value[0]
                    elif len(value) > 1:
                        result_data[key] = set(value)

                if 'dois' in result_data:
                    ds.attrs['doi'] = result_data['dois']
                if 'rors' in result_data:
                    ds.attrs['ror'] = result_data['rors']
                if 'funding_sources' in result_data:
                    ds.attrs['funder_text'] = result_data['funding_sources']

        return ds


def get_ror_display_name(ror_id: str) -> Optional[str]:
    """
    Parse ROR API response to find the for_display name of a given ROR ID.
    
    Args:
        ror_id (str): The ROR identifier (e.g., "https://ror.org/02jx3x895" or just "02jx3x895")
    
    Returns:
        Optional[str]: The for_display name if found, None otherwise
    """
    # Clean the ROR ID - extract just the identifier part if full URL is provided
    if ror_id.startswith('https://ror.org/'):
        ror_id = ror_id.replace('https://ror.org/', '')
    
    try:
        # Make request to ROR API
        url = f"https://api.ror.org/organizations/{ror_id}"
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Extract for_display name
        names = data.get('names', [])
        for name_entry in names:
            if name_entry.get('types') and 'ror_display' in name_entry['types']:
                return name_entry.get('value')
        
        # Fallback to primary name if no for_display found
        return data.get('name')
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from ROR API: {e}")
        return None
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error parsing ROR API response: {e}")
        return None

def generate_citation(ds: xr.Dataset) -> str:
    """
    Generate a citation string for the dataset based on its attributes.

    Parameters
    ----------
    ds : xr.Dataset
        The xarray Dataset containing metadata.

    Returns
    -------
    str
        A formatted citation string.
    """

    citation_string = ""
    any_citation_info = False

    citation_string += "== Data Citation ==\n"

    if 'ror' in ds.attrs and ds.attrs['ror']:
        any_citation_info = True
        if isinstance(ds.attrs['ror'], (set, list)):
            institution_name = ', '.join([get_ror_display_name(ror) for ror in ds.attrs['ror']])
        else:
            institution_name = get_ror_display_name(ds.attrs['ror'])

        citation_string += f"This data was collected by {institution_name}.\n"

    

    if 'doi' in ds.attrs and ds.attrs['doi']:
        any_citation_info = True
        citation_string += f"Please cite the dataset DOI: https://doi.org/{ds.attrs['doi']}\n"
    
    if 'funder_text' in ds.attrs and ds.attrs['funder_text']:
        any_citation_info = True
        citation_string += f"Please include the following funder acknowledgment:\n{ds.attrs['funder_text']}\n"

    if not any_citation_info:
        citation_string += "No specific citation information was retrieved for this dataset. By default, please cite:\n"
        citation_string += "CReSIS. 2024. REPLACE_WITH_RADAR_NAME Data, Lawrence, Kansas, USA. Digital Media. http://data.cresis.ku.edu/."

    # Add general OPR Toolbox citation
    citation_string += "\n== Processing Citation ==\n"
    citation_string += "Data was processed using the Open Polar Radar (OPR) Toolbox: https://doi.org/10.5281/zenodo.5683959\n"
    citation_string += "Please cite the OPR Toolbox as:\n"
    citation_string += "Open Polar Radar. (2024). opr (Version 3.0.1) [Computer software]. https://gitlab.com/openpolarradar/opr/. https://doi.org/10.5281/zenodo.5683959\n"
    citation_string += "And include the following acknowledgment:\n"
    citation_string += "We acknowledge the use of software from Open Polar Radar generated with support from the University of Kansas, NASA grants 80NSSC20K1242 and 80NSSC21K0753, and NSF grants OPP-2027615, OPP-2019719, OPP-1739003, IIS-1838230, RISE-2126503, RISE-2127606, and RISE-2126468.\n"

    return citation_string if citation_string else "No citation information available for this dataset."

def get_layers(ds: xr.Dataset) -> dict:
    """
    Fetch layer data from the OPS API and add it to the dataset.

    Parameters
    ----------
    ds : xr.Dataset
        The xarray Dataset containing radar data.

    Returns
    -------
    dict
        A dictionary mapping layer IDs to their corresponding data.
    """
    
    layer_points = xopr.ops_api.get_layer_points(
        segment_name=ds.attrs['segment'],
        season_name=ds.attrs['season'],
        location="antarctic" # TODO: Shouldn't be hardcoded
    )

    if layer_points['status'] != 1:
        print(layer_points)
        raise ValueError(f"Failed to fetch layer points. Received response with status {layer_points['status']}.")

    layer_ds_raw = xr.Dataset(
        {k: (['gps_time'], v) for k, v in layer_points['data'].items() if k != 'gps_time'},
        coords={'gps_time': layer_points['data']['gps_time']}
    )
    # Split into a dictionary of layers based on lyr_id
    layer_ids = set(layer_ds_raw['lyr_id'].to_numpy())
    layer_ids = [int(layer_id) for layer_id in layer_ids if not np.isnan(layer_id)]

    layers = {}
    for layer_id in layer_ids:
        l = layer_ds_raw.where(layer_ds_raw['lyr_id'] == layer_id, drop=True)

        l = l.sortby('gps_time')
        #l = l.rename({'twtt': f'layer_{layer_id}_twtt'})

        l = l.rename({'gps_time': 'slow_time'})
        l = l.set_coords(['slow_time'])

        # slow_time_1d = pd.to_datetime(l['slow_time'].values, unit='s')
        # l = l.assign_coords(slow_time=('slow_time_idx', slow_time_1d))

        l['slow_time'] = pd.to_datetime(l['slow_time'].values, unit='s')

        

        # Filter to the same time range as ds
        l = l.sel(slow_time=slice(ds['slow_time'].min(), ds['slow_time'].max()))

        layers[layer_id] = l
        #ds = xr.merge([ds, l], compat='override')

    return layers
        

    #return ds