from typing import Iterable
import xarray as xr
import fsspec
import pandas as pd
import numpy as np

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
            ds.Data.dims[1]: 'fast_time_idx',
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

        # Make fast_time and slow_time the indexing coordinates
        ds = ds.swap_dims({'fast_time_idx': 'twtt'})
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
                ds.attrs['doi'] = set(result['data']['dois'])
                ds.attrs['ror'] = set(result['data']['rors'])
                ds.attrs['funder_text'] = result['data']['funding_sources']

        return ds
    

def combine_attrs(variable_attrs: Iterable[dict], context = None) -> dict:
    """
    Merge metadata from multiple variable attributes into a single dictionary.

    Parameters
    ----------
    variable_attrs : iterable of dict
        An iterable containing dictionaries of variable attributes.

    Returns
    -------
    dict
        A dictionary containing the merged metadata.
    """
    merged = {}
    for attrs in variable_attrs:
        for key, value in attrs.items():
            if isinstance(value, np.ndarray):
                value = tuple(value)

            if key not in merged:
                merged[key] = set([value])
            else:
                try:
                    if np.any(np.isnan(value)) and np.any([np.isnan(v) for v in merged[key]]):
                        continue
                except TypeError:
                    pass

                merged[key].add(value)

    for key in merged:
        if len(merged[key]) == 1:
            merged[key] = next(iter(merged[key]))

    return merged


def get_institution(ror_code):
    import requests
    try:
        response = requests.get(f"https://api.ror.org/organizations/{ror_code}")
        return response.json()['name'] if response.status_code == 200 else f"ROR {ror_code} not found"
    except:
        return f"Error looking up ROR {ror_code}"

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

    if 'ror' in ds.attrs:
        if isinstance(ds.attrs['ror'], (set, list)):
            institution_name = ', '.join([get_institution(ror) for ror in ds.attrs['ror']])
        else:
            institution_name = get_institution(ds.attrs['ror'])
        
        citation_string += f"This data was collected by {institution_name}.\n"

    citation_string += "Data was processed using the Open Polar Radar (OPR) Toolbox: https://doi.org/10.5281/zenodo.5683959\n"

    if 'doi' in ds.attrs:
        citation_string += f"Please cite the dataset DOI: https://doi.org/{ds.attrs['doi']}\n"
    
    if 'funder_text' in ds.attrs:
        citation_string += f"Please include the following funder acknowledgment:\n{ds.attrs['funder_text']}\n"

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
        #l = l.rename({'twtt': f'layer_{layer_id}_twtt'})

        l = l.rename({'gps_time': 'slow_time'})
        l = l.set_coords(['slow_time'])

        slow_time_1d = pd.to_datetime(l['slow_time'].values, unit='s')
        l = l.assign_coords(slow_time=('slow_time', slow_time_1d))

        # Merge the twtt field into ds using the slow_time dimension from ds and
        # interpolating the layer data to match
        l = l.interp(slow_time=ds['slow_time'], method='linear')
        
        layers[layer_id] = l
        #ds = xr.merge([ds, l], compat='override')

    return layers
        

    #return ds
    

