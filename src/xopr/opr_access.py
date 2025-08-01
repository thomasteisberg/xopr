from typing import Iterable, Optional
import xarray as xr
import fsspec
import pandas as pd
import numpy as np
import requests
import json

from xopr.cf_units import apply_cf_compliant_attrs
import xopr.ops_api

class OPRConnection:
    def __init__(self,
                 collection_url: str = "https://data.cresis.ku.edu/data/",
                 cache_dir: str = None,
                 stac_api_url: str = "https://opr-stac-fastapi-974574526248.us-west1.run.app"):
        """
        Initialize the OPRConnection with a collection URL and optional cache directory.

        Parameters
        ----------
        collection_url : str
            The base URL for the OPR data collection.
        cache_dir : str, optional
            Directory to cache downloaded data.
        stac_api_url : str, optional
            The URL of the STAC API to use for metadata and item retrieval.
        """
        self.collection_url = collection_url
        self.cache_dir = cache_dir
        self.stac_api_url = stac_api_url

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
        
        if self.fsspec_url_prefix:
            file = fsspec.open_local(f"{self.fsspec_url_prefix}{url}", filecache=self.fsspec_cache_kwargs)
            
        else:
            file = fsspec.open_local(f"simplecache::{url}", **self.fsspec_cache_kwargs)

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

    def query_stac_collection(self, collection_id: str, exclude_geometry: bool = False) -> list:
        """
        Query STAC API to get all items in a collection using the /search endpoint.

        Parameters
        ----------
        collection_id : str
            The ID of the STAC collection to query.
        exclude_geometry : bool, optional
            If True, exclude geometry and bbox fields from the response to reduce size.

        Returns
        -------
        list
            List of STAC item dictionaries containing metadata and asset URLs.
        """
        # Use STAC /search endpoint instead of /collections endpoint
        search_url = f"{self.stac_api_url}/search"
        
        all_items = []
        limit = 500  # Use a reasonable batch size
        next_url = None
        previous_feature_ids = None
        current_offset = 0  # Track offset manually for POST requests
        
        while True:
            try:
                if exclude_geometry:
                    # When excluding geometry, always use POST requests to ensure proper field exclusion
                    current_url = search_url
                    search_body = {
                        'collections': [collection_id],
                        'limit': limit,
                        'fields': {
                            'exclude': ['geometry', 'bbox']
                        }
                    }
                    
                    # Add offset for pagination (manual tracking since server doesn't provide proper next URLs for POST)
                    if current_offset > 0:
                        search_body['offset'] = current_offset

                    print(f"Querying STAC API: {current_url} with POST body: {search_body}")
                    response = requests.post(current_url, json=search_body)
                else:
                    # When not excluding geometry, use GET requests (more efficient)
                    if next_url:
                        current_url = next_url
                        params = {}  # Next URL should already contain all necessary parameters
                    else:
                        current_url = search_url
                        params = {
                            'collections': collection_id,
                            'limit': limit
                        }

                    print(f"Querying STAC API: {current_url} with params: {params}")
                    response = requests.get(current_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Add items from this page
                features = data.get('features', [])
                print(f"Found {len(features)} features in this page.")
                if not features:
                    # No more items, break the loop
                    break
                
                # Check for duplicate features from previous page
                current_feature_ids = {feature.get('id') for feature in features}
                if previous_feature_ids is not None and current_feature_ids == previous_feature_ids:
                    raise RuntimeError(f"STAC API returned duplicate features across pages. "
                                     f"This indicates a server pagination bug. "
                                     f"Collection: {collection_id}, Feature IDs: {current_feature_ids}")
                
                all_items.extend(features)
                previous_feature_ids = current_feature_ids
                
                if exclude_geometry:
                    # For POST requests, manually handle pagination since server doesn't provide proper next URLs
                    if len(features) < limit:
                        # Received fewer features than requested, we've reached the end
                        break
                    else:
                        # Increment offset for next request
                        current_offset += limit
                else:
                    # For GET requests, use the server's next link
                    next_url = None
                    if 'links' in data:
                        for link in data['links']:
                            if link.get('rel') == 'next':
                                next_url = link.get('href')
                                print(f"Next page URL found: {next_url}")
                                break
                    
                    # If no next link, we've reached the end
                    if not next_url:
                        break
                            
            except requests.exceptions.RequestException as e:
                print(f"Error querying STAC API: {e}")
                break
            except json.JSONDecodeError as e:
                print(f"Error parsing STAC API response: {e}")
                break
        
        return all_items

    def query_flight_items(self, collection_id: str, date_str: str, flight_num: int) -> list:
        """
        Query STAC API for items from a specific flight using CQL2 filtering.

        Parameters
        ----------
        collection_id : str
            The ID of the STAC collection to query.
        date_str : str
            The flight date in YYYYMMDD format.
        flight_num : int
            The flight number.

        Returns
        -------
        list
            List of STAC item dictionaries for the specified flight.
        """
        search_url = f"{self.stac_api_url}/search"
        
        all_items = []
        next_url = None
        previous_feature_ids = None
        
        while True:
            try:
                # Build CQL2 filter for specific flight
                filter_condition = {
                    "op": "and",
                    "args": [
                        {
                            "op": "=",
                            "args": [{"property": "opr:date"}, date_str]
                        },
                        {
                            "op": "=", 
                            "args": [{"property": "opr:flight"}, flight_num]
                        }
                    ]
                }
                
                # Use the next URL if available, otherwise build search request
                if next_url:
                    current_url = next_url
                    # For next URLs, make a GET request (they should contain all parameters)
                    response = requests.get(current_url)
                else:
                    # Initial request with POST and CQL2 filter
                    search_body = {
                        'collections': [collection_id],
                        'limit': 100,
                        'filter': filter_condition
                    }
                    response = requests.post(search_url, json=search_body)
                
                response.raise_for_status()
                data = response.json()
                
                # Add items from this page
                features = data.get('features', [])
                if not features:
                    break
                
                # Check for duplicate features from previous page
                current_feature_ids = {feature.get('id') for feature in features}
                if previous_feature_ids is not None and current_feature_ids == previous_feature_ids:
                    raise RuntimeError(f"STAC API returned duplicate features across pages. "
                                     f"Collection: {collection_id}, Date: {date_str}, Flight: {flight_num}")
                
                all_items.extend(features)
                previous_feature_ids = current_feature_ids
                
                # Look for next link
                next_url = None
                if 'links' in data:
                    for link in data['links']:
                        if link.get('rel') == 'next':
                            next_url = link.get('href')
                            break
                
                if not next_url:
                    break
                            
            except requests.exceptions.RequestException as e:
                print(f"Error querying STAC API for flight {date_str}_{flight_num:02d}: {e}")
                break
            except json.JSONDecodeError as e:
                print(f"Error parsing STAC API response: {e}")
                break
        
        return all_items

    def get_collections(self) -> list:
        """
        Get list of available STAC collections.

        Returns
        -------
        list
            List of collection dictionaries with metadata.
        """
        collections_url = f"{self.stac_api_url}/collections"
        
        try:
            response = requests.get(collections_url)
            response.raise_for_status()
            data = response.json()
            return data.get('collections', [])
            
        except requests.exceptions.RequestException as e:
            print(f"Error querying STAC API for collections: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"Error parsing STAC API response: {e}")
            return []

    def get_flights(self, collection_id: str) -> list:
        """
        Get list of available flights within a collection/season.

        Parameters
        ----------
        collection_id : str
            The ID of the STAC collection to query.

        Returns
        -------
        list
            List of flight dictionaries with flight metadata.
        """
        # Query STAC API for all items in collection (exclude geometry for better performance)
        items = self.query_stac_collection(collection_id, exclude_geometry=True)
        
        if not items:
            print(f"No items found in collection '{collection_id}'")
            return []
        
        # Group items by flight (opr:date + opr:flight)
        flights = {}
        for item in items:
            properties = item.get('properties', {})
            date = properties.get('opr:date')
            flight_num = properties.get('opr:flight')
            
            if date and flight_num is not None:
                flight_key = f"{date}_{flight_num:02d}"
                
                if flight_key not in flights:
                    flights[flight_key] = {
                        'flight_id': flight_key,
                        'date': date,
                        'flight_number': flight_num,
                        'collection': collection_id,
                        'segments': [],
                        'item_count': 0
                    }
                
                flights[flight_key]['segments'].append(properties.get('opr:segment'))
                flights[flight_key]['item_count'] += 1
        
        # Sort flights by date and flight number
        flight_list = list(flights.values())
        flight_list.sort(key=lambda x: (x['date'], x['flight_number']))
        
        return flight_list

    def load_flight(self, collection_id: str, flight_id: str) -> list:
        """
        Load all radar frames from a specific flight.

        Parameters
        ----------
        collection_id : str
            The ID of the STAC collection containing the flight.
        flight_id : str
            The flight ID (format: YYYYMMDD_NN, e.g., '20161014_03').

        Returns
        -------
        list
            List of xarray Datasets, one for each frame in the flight, sorted by segment.
        """
        # Parse flight_id to get date and flight number
        try:
            date_str, flight_num_str = flight_id.split('_')
            flight_num = int(flight_num_str)
        except ValueError:
            print(f"Invalid flight_id format '{flight_id}'. Expected format: YYYYMMDD_NN")
            return []
        
        # Query STAC API with CQL2 filter for specific flight
        flight_items = self.query_flight_items(collection_id, date_str, flight_num)
        
        if not flight_items:
            print(f"No items found for flight '{flight_id}' in collection '{collection_id}'")
            return []
        
        # Sort items by segment number
        flight_items.sort(key=lambda x: x.get('properties', {}).get('opr:segment', 0))
        
        print(f"Loading {len(flight_items)} frames from flight '{flight_id}'...")
        
        # Load each frame
        frames = []
        for i, item in enumerate(flight_items):
            try:
                # Get data asset URL
                data_asset = item.get('assets', {}).get('data')
                if not data_asset:
                    print(f"Warning: No data asset found for item {item.get('id', 'unknown')}")
                    continue
                
                url = data_asset.get('href')
                if not url:
                    print(f"Warning: No href found for data asset in item {item.get('id', 'unknown')}")
                    continue
                
                # Load the frame
                frame = self.load_frame(url)
                
                # Add STAC metadata to frame attributes
                frame.attrs['stac_collection'] = collection_id
                frame.attrs['stac_item_id'] = item.get('id')
                frame.attrs['flight_id'] = flight_id
                
                # Add OPR properties if available
                properties = item.get('properties', {})
                if 'opr:date' in properties:
                    frame.attrs['opr_date'] = properties['opr:date']
                if 'opr:flight' in properties:
                    frame.attrs['opr_flight'] = properties['opr:flight']
                if 'opr:segment' in properties:
                    frame.attrs['opr_segment'] = properties['opr:segment']
                
                frames.append(frame)
                
                if (i + 1) % 5 == 0:
                    print(f"  Loaded {i + 1}/{len(flight_items)} frames...")
                    
            except Exception as e:
                print(f"Error loading frame for item {item.get('id', 'unknown')}: {e}")
                continue
        
        print(f"Successfully loaded {len(frames)} frames from flight '{flight_id}'")
        return frames


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