from typing import Iterable, Optional, Union
import warnings
import xarray as xr
import fsspec
import pandas as pd
import numpy as np
import requests
import re
import json
import scipy.io
import geopandas as gpd
import shapely
import pystac_client
import pystac
import h5py

from .cf_units import apply_cf_compliant_attrs
from .matlab_attribute_utils import decode_hdf5_matlab_variable, extract_legacy_mat_attributes
from .util import merge_dicts_no_conflicts
from . import ops_api

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

    def query_frames(self, seasons: list[str] = None, flight_ids: list[str] = None,
                     geometry = None, date_range: tuple = None,
                     properties: dict = {}, max_items: int = None, full_flights: bool = False,
                     exclude_geometry: bool = False, return_type='dict', return_iterator=False,
                     search_kwargs: dict = {}) -> list:
        """
        Query for radar frames based on various search criteria. All criteria are combined with AND logic.
        Lists passed to seasons or flight_ids are combined with OR logic.

        If full_flights is True, all frames from any flight matching the criteria will be included.

        Parameters
        ----------
        seasons : list[str], optional
            List of season names to filter by (e.g., "2022_Antarctica_BaslerMKB").
        flight_ids : list[str], optional
            List of flight IDs to filter by (e.g., "20230126_01").
        geometry : optional
            Geospatial geometry to filter by (e.g., a shapely geometry object).
        date_range : tuple, optional
            Date range to filter by (e.g., (start_date, end_date)).
        properties : dict, optional
            Additional properties to include in the query.
        max_items : int, optional
            Maximum number of items to return.
        full_flights : bool, optional
            If True, return all frames from matching flights.
        exclude_geometry : bool, optional
            If True, exclude the geometry field from the response to reduce size.
        return_type : str, optional
            The type of response to return. Options are 'dict', 'pystac', or 'search'.
            'search' will return a pystac_client.Search object that can be manually
            queried.
        return_iterator : bool, optional
            If True, return an iterator over the results instead of a list.
            This is useful for working with large result sets.
        search_kwargs : dict, optional
            Additional keyword arguments to pass to the search method.

        Returns
        -------
        list[dict]
            List of STAC frames matching the query criteria.
        """

        # Set up STAC client for normal queries
        client = pystac_client.Client.open(self.stac_api_url)
        search_params = {}

        # Use direct requests when excluding geometry, otherwise use pystac_client
        if exclude_geometry:
            search_params['fields'] = ['-geometry']
        
        # Handle collections (seasons)
        if seasons is not None:
            if isinstance(seasons, str):
                seasons = [seasons]
            search_params['collections'] = seasons
        
        # Handle geometry filtering
        if geometry is not None:
            search_params['intersects'] = geometry
        
        # Handle date range filtering
        if date_range is not None:
            search_params['datetime'] = date_range

        # Handle max_items
        if max_items is not None:
            search_params['max_items'] = max_items

        # Handle flight_ids filtering using CQL2
        filter_conditions = []
        
        if flight_ids is not None:
            if isinstance(flight_ids, str):
                flight_ids = [flight_ids]
            
            # Create OR conditions for flight IDs
            flight_conditions = []
            for flight_id in flight_ids:
                try:
                    date_str, flight_num_str = flight_id.split('_')
                    flight_num = int(flight_num_str)
                    
                    # Create AND condition for this specific flight
                    flight_condition = {
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
                    flight_conditions.append(flight_condition)
                except ValueError:
                    print(f"Warning: Invalid flight_id format '{flight_id}'. Expected format: YYYYMMDD_NN")
                    continue
            
            if flight_conditions:
                if len(flight_conditions) == 1:
                    filter_conditions.append(flight_conditions[0])
                else:
                    # Multiple flights - combine with OR
                    filter_conditions.append({
                        "op": "or",
                        "args": flight_conditions
                    })

        # Add any additional property filters
        for key, value in properties.items():
            filter_conditions.append({
                "op": "=",
                "args": [{"property": key}, value]
            })
        
        # Combine all filter conditions with AND
        if filter_conditions:
            if len(filter_conditions) == 1:
                filter_expr = filter_conditions[0]
            else:
                filter_expr = {
                    "op": "and",
                    "args": filter_conditions
                }
            
            search_params['filter'] = filter_expr
        
        # Add any extra kwargs to search
        search_params.update(search_kwargs)

        # Perform the search
        search = client.search(**search_params)
        if return_type == 'dict':
            items = search.items_as_dicts()
        elif return_type == 'pystac':
            items = search.items()
        elif (not full_flights) and (return_type == 'search'):
            return search
        else:
            raise ValueError(f"Unsupported return_type: {return_type}. Use 'dict', 'pystac', or 'search'.")

        if not return_iterator:
            items = list(items)
        
        if items and not full_flights:
            return items
        elif items and full_flights:
            # Get all flights that match the criteria, then get all frames from those flights
            matching_flights = set()
            for item in items:
                # Handle both dict items (from direct requests) and pystac Item objects
                if isinstance(item, dict):
                    properties = item.get('properties', {})
                    collection = item.get('collection')
                else:
                    properties = item.properties
                    collection = item.collection_id
                
                date = properties.get('opr:date')
                flight_num = properties.get('opr:flight')
                
                if date and flight_num is not None and collection:
                    flight_key = f"{date}_{flight_num:02d}"
                    matching_flights.add((collection, flight_key))
            
            # Recursively call query_frames for each flight to get all frames
            all_flight_frames = []
            for collection, flight_id in matching_flights:
                flight_frames = self.query_frames(
                    seasons=[collection],
                    flight_ids=[flight_id],
                    max_items=None,
                    full_flights=False,  # Avoid infinite recursion
                    exclude_geometry=exclude_geometry,  # Preserve the exclude_geometry setting
                    return_type=return_type,
                    return_iterator=return_iterator,
                    search_kwargs=search_kwargs
                )
                all_flight_frames.extend(flight_frames)
            
            print(f"Expanded to {len(all_flight_frames)} frames from {len(matching_flights)} full flights")
            return all_flight_frames
        else:
            return []

    def load_frames(self, stac_items: list[dict],
                    data_product: str = "CSARP_standard",
                    merge_flights: bool = False,
                    skip_errors: bool = False,
                    ) -> list[xr.Dataset]:
        """
        Load multiple radar frames from a list of STAC items.

        Parameters
        ----------
        stac_items : list[dict]
            List of STAC item dictionaries containing metadata and asset URLs.
        data_product : str, optional
            The data product to load (default is "CSARP_standard").
        merge_flights : bool, optional
            Whether to merge frames from the same flight (default is False).
        skip_errors : bool, optional
            Whether to skip errors and continue loading other frames (default is False).

        Returns
        -------
        list[xr.Dataset]
            List of loaded radar frames as xarray Datasets.
        """
        frames = []
        
        for item in stac_items:
            try:
                frame = self.load_frame(item, data_product)
                frames.append(frame)
            except Exception as e:
                print(f"Error loading frame for item {item.get('id', 'unknown')}: {e}")
                if skip_errors:
                    continue
                else:
                    raise e

        if merge_flights:
            return self.merge_flights_from_frames(frames)
        else:
            return frames

    def load_frame(self, stac_item, data_product: str = "CSARP_standard") -> xr.Dataset:
        """
        Load a radar frame from a STAC item.

        Parameters
        ----------
        stac_item : dict or pystac.Item
            The STAC item containing asset URLs.
        data_product : str, optional
            The data product to load (default is "CSARP_standard").

        Returns
        -------
        xr.Dataset
            The loaded radar frame as an xarray Dataset.
        """
        # Handle both dict and pystac.Item objects
        if hasattr(stac_item, 'assets'):
            # pystac.Item object
            assets = stac_item.assets
        else:
            # Dict object
            assets = stac_item.get('assets', {})
        
        # Get the data asset
        data_asset = assets.get(data_product)
        if not data_asset:
            available_assets = list(assets.keys())
            raise ValueError(f"No {data_product} asset found. Available assets: {available_assets}")
        
        # Get the URL from the asset
        if hasattr(data_asset, 'href'):
            # pystac.Asset object
            url = data_asset.href
        else:
            # Dict object
            url = data_asset.get('href')
            if not url:
                raise ValueError(f"No href found in {data_product} asset")
        
        # Load the frame using the existing method
        return self.load_frame_url(url)

    def load_frame_url(self, url: str) -> xr.Dataset:
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


        filetype = None
        try:
            ds = self._load_frame_hdf5(file)
            filetype = 'hdf5'
        except OSError:
            ds = self._load_frame_matlab(file)
            filetype = 'matlab'

        # Add the source URL as an attribute
        ds.attrs['source_url'] = url

        # Apply CF-compliant attributes
        ds = apply_cf_compliant_attrs(ds)

        # Get the season and segment from the URL
        match = re.search(r'(\d{4}_\w+_[A-Za-z0-9]+)\/[\w_]+\/([\d_]+)', url)
        if match:
            season, segment = match.groups()
            ds.attrs['season'] = season
            ds.attrs['segment'] = segment

            ds['src_season'] = xr.DataArray(
                [season] * ds.sizes['slow_time'],
                dims=['slow_time'],
                coords={'slow_time': ds.coords['slow_time']},
                attrs={'description': 'Season name from source URL'}
            )
            ds['src_segment'] = xr.DataArray(
                [segment] * ds.sizes['slow_time'],
                dims=['slow_time'],
                coords={'slow_time': ds.coords['slow_time']},
                attrs={'description': 'Segment name from source URL'}
            )

            # Load citation information
            result = ops_api.get_segment_metadata(segment_name=segment, season_name=season)
            if result:
                if isinstance(result['data'], str):
                    warnings.warn(f"Warning: Unexpected result from ops_api: {result['data']}", UserWarning)
                else:
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

        # Add the rest of the Matlab parameters
        if filetype == 'hdf5':
            ds.attrs['mimetype'] = 'application/x-hdf5'
            ds.attrs.update(decode_hdf5_matlab_variable(h5py.File(file, 'r'),
                                                        skip_variables=True,
                                                        skip_errors=True))
        elif filetype == 'matlab':
            ds.attrs['mimetype'] = 'application/x-matlab-data'
            ds.attrs.update(extract_legacy_mat_attributes(file,
                                                          skip_keys=ds.keys(),
                                                          skip_errors=True))

        return ds
    
    def _load_frame_hdf5(self, file) -> xr.Dataset:
        """
        Load a radar frame from an HDF5 file.

        Parameters
        ----------
        file : 
            The path to the HDF5 file containing radar frame data.

        Returns
        -------
        xr.Dataset
            The loaded radar frame as an xarray Dataset.
        """

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

        return ds
    
    def _load_frame_matlab(self, file) -> xr.Dataset:
        """
        Load a radar frame from a MATLAB file.

        Parameters
        ----------
        file : 
            The path to the MATLAB file containing radar frame data.

        Returns
        -------
        xr.Dataset
            The loaded radar frame as an xarray Dataset.
        """
        
        m = scipy.io.loadmat(file, mat_dtype=False)

        key_dims = {
            'Time': ('twtt',),
            'GPS_time': ('slow_time',),
            'Latitude': ('slow_time',),
            'Longitude': ('slow_time',),
            'Elevation': ('slow_time',),
            'Roll': ('slow_time',),
            'Pitch': ('slow_time',),
            'Heading': ('slow_time',),
            'Surface': ('slow_time',),
            'Data': ('twtt', 'slow_time')
        }
        
        ds = xr.Dataset(
            {
                key: (dims, np.squeeze(m[key])) for key, dims in key_dims.items() if key in m
            },
            coords={
                'twtt': ('twtt', np.squeeze(m['Time'])),
                'slow_time': ('slow_time', pd.to_datetime(np.squeeze(m['GPS_time']), unit='s')),
            }
        )

        return ds

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
        items = self.query_frames(seasons=[collection_id], exclude_geometry=True)
        
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
    
    def merge_flights_from_frames(self, frames: Iterable[xr.Dataset]) -> list[xr.Dataset]:
        """
        Merge a set of radar frames into a list of merged xarray Datasets.

        Parameters
        ----------
        frames : Iterable[xr.Dataset]
            An iterable of xarray Datasets representing radar frames.

        Returns
        -------
        list[xr.Dataset]
            List of merged xarray Datasets.
        """
        flights = {}
        
        for frame in frames:
            # Get flight ID from frame attributes
            flight_id = frame.attrs.get('segment')
            if not flight_id:
                print("Warning: Frame missing 'segment' attribute, skipping.")
                continue
            
            if flight_id not in flights:
                flights[flight_id] = []
            
            flights[flight_id].append(frame)
        
        # Merge frames for each flight
        merged_flights = []
        for flight_id, flight_frames in flights.items():
            merged_flight = xr.concat(flight_frames, dim='slow_time', combine_attrs=merge_dicts_no_conflicts).sortby('slow_time')
            merged_flights.append(merged_flight)

        return merged_flights


    def get_ror_display_name(self, ror_id: str) -> Optional[str]:
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

    def generate_citation(self, ds: xr.Dataset) -> str:
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
                institution_name = ', '.join([self.get_ror_display_name(ror) for ror in ds.attrs['ror']])
            else:
                institution_name = self.get_ror_display_name(ds.attrs['ror'])

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

    def get_layers_files(self, flight: Union[xr.Dataset, dict, pystac.Item]) -> dict:
        """
        Fetch layers from the CSARP_layers files
        
        Parameters
        ----------
        flight : Union[xr.Dataset, dict, pystac.Item]
            The flight information, which can be an xarray Dataset, a dictionary, or a STAC item.

        Returns
        -------
        dict
            A dictionary mapping layer IDs to their corresponding data.
        """
        if isinstance(flight, xr.Dataset):
            # Get collection and flight information from the dataset attributes
            collection = flight.attrs.get('season')
            flight_id = flight.attrs.get('segment')
            frame = None # Could be multiple frames in the dataset
        else:
            if isinstance(flight, pystac.Item):
                flight = flight.to_dict()
            collection = flight['collection']
            flight_id = f"{flight['properties'].get('opr:date')}_{flight['properties'].get('opr:flight'):02d}" # TODO: Update after resolving https://github.com/thomasteisberg/xopr/issues/22
            frame = flight['properties'].get('opr:segment')

        properties = {}
        if frame:
            properties['opr:segment'] = frame

        # Query STAC collection for CSARP_layer files matching this specific flight
                      
        # Get items from this specific flight
        stac_items = self.query_frames(seasons=[collection], flight_ids=[flight_id], properties=properties)

        # Filter for items that have CSARP_layer assets
        layer_items = []
        for item in stac_items:
            if 'CSARP_layer' in item.get('assets', {}):
                layer_items.append(item)
        
        if not layer_items:
            #print(f"No CSARP_layer files found for segment {flight_id} in collection {collection}")
            return {}
        
        # Load each layer file and combine them
        layer_frames = []
        for item in layer_items:
            layer_asset = item.get('assets', {}).get('CSARP_layer')
            if layer_asset and 'href' in layer_asset:
                url = layer_asset['href']
                try:
                    layer_ds = self.load_layers_file(url)
                    layer_frames.append(layer_ds)
                except Exception as e:
                    print(f"Warning: Failed to load layer file {url}: {e}")
                    continue
        
        if not layer_frames:
            print("No layer frames could be loaded")
            return {}
        
        # Concatenate all layer frames along slow_time dimension
        layers_flight = xr.concat(layer_frames, dim='slow_time', combine_attrs='drop_conflicts', data_vars='all')
        layers_flight = layers_flight.sortby('slow_time')
        
        # Trim to bounds of the original dataset
        layers_flight = self._trim_to_bounds(layers_flight, flight)
        
        # Split into separate layers by ID
        layers = {}
        
        layer_ids = np.unique(layers_flight['id'])
        
        for i, layer_id in enumerate(layer_ids):
            layer_id_int = int(layer_id)
            layer_data = {}
            
            for var_name, var_data in layers_flight.data_vars.items():
                if 'layer' in var_data.dims:
                    # Select the i-th layer from 2D variables (layer, slow_time)
                    layer_data[var_name] = (['slow_time'], var_data.isel(layer=i).values)
                else:
                    # 1D variables that don't have layer dimension
                    layer_data[var_name] = var_data
            
            # Create coordinates (excluding layer coordinate)
            coords = {k: v for k, v in layers_flight.coords.items() if k != 'layer'}
            
            # Create the layer dataset
            layer_ds = xr.Dataset(layer_data, coords=coords)
            layers[layer_id_int] = layer_ds
        
        return layers

    def _trim_to_bounds(self, ds: xr.Dataset, ref: Union[xr.Dataset, dict, pystac.Item]) -> xr.Dataset:
        start_time, end_time = None, None
        if isinstance(ref, xr.Dataset) and 'slow_time' in ref.coords:
            start_time = ref['slow_time'].min()
            end_time = ref['slow_time'].max()
        else:
            if isinstance(ref, pystac.Item):
                ref = ref.to_dict()
            properties = ref.get('properties', {})
            if 'start_datetime' in properties and 'end_datetime' in properties:
                start_time = pd.to_datetime(properties['start_datetime'])
                end_time = pd.to_datetime(properties['end_datetime'])

        if start_time:
            return ds.sel(slow_time=slice(start_time, end_time))
        else:
            return ds

    def load_layers_file(self, url: str) -> xr.Dataset:
        """
        Load layer data from a CSARP_layer file (either HDF5 or MATLAB format).
        
        Parameters
        ----------
        url : str
            URL or path to the layer file
            
        Returns
        -------
        xr.Dataset
            Layer data in a standardized format with coordinates:
            - slow_time: GPS time converted to datetime
            And data variables:
            - twtt: Two-way travel time for each layer
            - quality: Quality values for each layer
            - type: Type values for each layer  
            - lat, lon, elev: Geographic coordinates
            - id: Layer IDs
        """

        if self.fsspec_url_prefix:
            file = fsspec.open_local(f"{self.fsspec_url_prefix}{url}", filecache=self.fsspec_cache_kwargs)
        else:
            file = fsspec.open_local(f"simplecache::{url}", **self.fsspec_cache_kwargs)

        try:
            ds = self._load_layers_hdf5(file)
        except OSError:
            ds = self._load_layers_matlab(file)

        # Add the source URL as an attribute
        ds.attrs['source_url'] = url

        # Apply common manipulations to match the expected structure
        # Convert GPS time to datetime coordinate
        if 'gps_time' in ds.variables:
            slow_time_dt = pd.to_datetime(ds['gps_time'].values, unit='s')
            ds = ds.assign_coords(slow_time=('slow_time', slow_time_dt))
            
            # Set slow_time as the main coordinate and remove gps_time from data_vars
            if 'slow_time' not in ds.dims:
                ds = ds.swap_dims({'gps_time': 'slow_time'})
            
            # Remove gps_time from data_vars if it exists there to avoid conflicts
            if ('gps_time' in ds.data_vars) or ('gps_time' in ds.coords):
                ds = ds.drop_vars('gps_time')
        
        # Sort by slow_time if it exists
        if 'slow_time' in ds.coords:
            ds = ds.sortby('slow_time')

        return ds
    
    def _load_layers_hdf5(self, file) -> xr.Dataset:
        """
        Load layer data from an HDF5 format file.
        
        Parameters
        ----------
        file : str
            Path to the HDF5 layer file
            
        Returns
        -------
        xr.Dataset
            Raw layer data from HDF5 file
        """
        # Load the HDF5 file using h5netcdf engine with phony_dims='sort'
        ds = xr.open_dataset(file, engine='h5netcdf', phony_dims='sort')
        
        # Squeeze to remove singleton dimensions
        ds = ds.squeeze()
        
        # Rename dimensions based on the structure observed in the notebook
        # The HDF5 format has phony dimensions that need to be renamed
        dim_mapping = {}
        
        # Find the dimension corresponding to the number of GPS time points
        if 'gps_time' in ds.variables:
            gps_time_shape = ds['gps_time'].shape
            if len(gps_time_shape) >= 1:
                gps_time_dim = ds['gps_time'].dims[0]
                dim_mapping[gps_time_dim] = 'slow_time'
        
        # Find the dimension corresponding to layer IDs
        if 'id' in ds.variables:
            id_shape = ds['id'].shape  
            if len(id_shape) >= 1:
                id_dim = ds['id'].dims[0]
                if id_dim not in dim_mapping.values():
                    dim_mapping[id_dim] = 'layer'
        
        # Apply dimension renaming
        if dim_mapping:
            ds = ds.rename(dim_mapping)
        
        # Set up coordinates and data variables separately
        coords = {}
        data_vars = {}
        
        # Handle coordinates
        if 'gps_time' in ds.variables:
            coords['gps_time'] = ds['gps_time']
            
        # Handle data variables, excluding coordinates
        for var_name, var_data in ds.data_vars.items():
            if var_name == 'gps_time':
                continue  # Skip gps_time since it's already in coords
            elif var_name in ['twtt', 'quality', 'type'] and len(var_data.dims) == 2:
                # These are 2D arrays: (layer, slow_time)
                data_vars[var_name] = var_data.transpose('slow_time', 'layer') if 'layer' in var_data.dims else var_data
            else:
                data_vars[var_name] = var_data
                
        # Create new dataset with proper structure
        ds_restructured = xr.Dataset(data_vars, coords=coords)
        
        return ds_restructured
    
    def _load_layers_matlab(self, file) -> xr.Dataset:
        """
        Load layer data from a MATLAB format file.
        
        Parameters
        ----------
        file : str
            Path to the MATLAB layer file
            
        Returns
        -------
        xr.Dataset  
            Raw layer data from MATLAB file
        """
        # Load MATLAB file
        m = scipy.io.loadmat(file, squeeze_me=True)
        
        # Extract basic 1D variables (same length as GPS time points)
        n_time = len(m['gps_time']) if 'gps_time' in m else 0
        
        data_vars = {}
        coords = {}
        
        # Handle coordinates
        if 'gps_time' in m:
            coords['gps_time'] = (['slow_time'], m['gps_time'])
        
        # Handle 1D variables that correspond to GPS time points
        for var_name in ['lat', 'lon', 'elev']:
            if var_name in m and np.asarray(m[var_name]).shape == (n_time,):
                data_vars[var_name] = (['slow_time'], m[var_name])
        
        # Handle the layer ID array
        if 'id' in m:
            layer_ids = np.asarray(m['id'])
            coords['layer'] = (['layer'], layer_ids)
            data_vars['id'] = (['layer'], layer_ids)
        
        # Handle 2D variables (layer x time)
        for var_name in ['twtt', 'quality', 'type']:
            if var_name in m:
                var_data = np.asarray(m[var_name])
                if var_data.ndim == 2:
                    # Shape is (layer, slow_time)
                    data_vars[var_name] = (['layer', 'slow_time'], var_data)
                elif var_data.ndim == 1:
                    # Sometimes these might be 1D
                    data_vars[var_name] = (['slow_time'], var_data)
        
        # Handle any other scalar or metadata variables
        for var_name in ['file_type', 'file_version']:
            if var_name in m and np.asarray(m[var_name]).ndim == 0:
                data_vars[var_name] = ([], m[var_name])
        
        # Create the dataset
        ds = xr.Dataset(data_vars, coords=coords)
        
        return ds

    def get_layers_db(self, flight: Union[xr.Dataset, dict, pystac.Item], include_geometry=True) -> dict:
        """
        Fetch layer data from the OPS API

        Parameters
        ----------
        flight : Union[xr.Dataset, dict, pystac.Item]
            The flight data, which can be an xarray Dataset, a dictionary, or a STAC item.
        include_geometry : bool, optional
            If True, include geometry information in the returned layers.

        Returns
        -------
        dict
            A dictionary mapping layer IDs to their corresponding data.
        """

        if isinstance(flight, xr.Dataset):
            # Get collection and flight information from the dataset attributes
            collection = flight.attrs.get('season')
            flight_id = flight.attrs.get('segment')
        else:
            if isinstance(flight, pystac.Item):
                flight = flight.to_dict()
            collection = flight['collection']
            flight_id = f"{flight['properties'].get('opr:date')}_{flight['properties'].get('opr:flight'):02d}" # TODO: Update after resolving https://github.com/thomasteisberg/xopr/issues/22

        if 'Antarctica' in collection:
            location = 'antarctic'
        elif 'Greenland' in collection:
            location = 'arctic'
        else:
            raise ValueError("Dataset does not belong to a recognized location (Antarctica or Greenland).")
        
        layer_points = ops_api.get_layer_points(
            segment_name=flight_id,
            season_name=collection,
            location=location,
            include_geometry=include_geometry
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

            l = l.rename({'gps_time': 'slow_time'})
            l = l.set_coords(['slow_time'])

            l['slow_time'] = pd.to_datetime(l['slow_time'].values, unit='s')

            # Filter to the same time range as flight
            l = self._trim_to_bounds(l, flight)

            layers[layer_id] = l

        return layers
        