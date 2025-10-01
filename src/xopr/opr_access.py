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
import h5py
import antimeridian
from rustac import DuckdbClient

from .cf_units import apply_cf_compliant_attrs
from .matlab_attribute_utils import decode_hdf5_matlab_variable, extract_legacy_mat_attributes
from .util import merge_dicts_no_conflicts
from . import ops_api
from . import opr_tools

class OPRConnection:
    def __init__(self,
                 collection_url: str = "https://data.cresis.ku.edu/data/",
                 cache_dir: str = None,
                 stac_parquet_href: str = "gs://opr_stac/catalog/**/*.parquet"):
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
        self.stac_parquet_href = stac_parquet_href

        self.fsspec_cache_kwargs = {}
        self.fsspec_url_prefix = ''
        if cache_dir:
            self.fsspec_cache_kwargs = {
                'cache_storage': cache_dir,
                'check_files': True
            }
            self.fsspec_url_prefix = 'filecache::'

    def query_frames(self, collections: list[str] = None, segment_paths: list[str] = None,
                     geometry = None, date_range: tuple = None, properties: dict = {},
                     max_items: int = None, exclude_geometry: bool = False,
                     search_kwargs: dict = {}) -> gpd.GeoDataFrame:
        """
        Query for radar frames based on various search criteria. Each parameter is
        treated as an independent criteria. If multiple parameters are passed, they are
        combined with AND logic.
        
        A list of values may be passed to most parameters. If so, any values in the list
        will be treated as a match.

        Parameters
        ----------
        collections : list[str], optional
            List of collection names to filter by (e.g., "2022_Antarctica_BaslerMKB").
        segment_paths : list[str], optional
            List of segment paths to filter by (e.g., "20230126_01").
        geometry : optional
            Geospatial geometry to filter by (e.g., a shapely geometry object).
        date_range : tuple, optional
            Date range to filter by (e.g., (start_date, end_date)).
        properties : dict, optional
            Additional properties to include in the query.
        max_items : int, optional
            Maximum number of items to return.
        exclude_geometry : bool, optional
            If True, exclude the geometry field from the response to reduce size.
        search_kwargs : dict, optional
            Additional keyword arguments to pass to the search method.

        Returns
        -------
        gpd.GeoDataFrame
            GeoDataFrame containing the STAC frames matching the query criteria.
        """

        search_params = {}

        # Exclude geometry -- do not return the geometry field to reduce response size
        if exclude_geometry:
            search_params['exclude'] = ['geometry']
        
        # Handle collections (seasons)
        if collections is not None:
            if isinstance(collections, str):
                collections = [collections]
            search_params['collections'] = collections

        # Handle geometry filtering
        if geometry is not None:
            if hasattr(geometry, '__geo_interface__'):
                geometry = geometry.__geo_interface__

            # Fix geometries that cross the antimeridian
            geometry = antimeridian.fix_geojson(geometry, reverse=True)

            search_params['intersects'] = geometry

        # Handle date range filtering
        if date_range is not None:
            search_params['datetime'] = date_range

        # Handle max_items
        if max_items is not None:
            search_params['limit'] = max_items
        else:
            search_params['limit'] = 1000000

        # Handle segment_paths filtering using CQL2
        filter_conditions = []

        if segment_paths is not None:
            if isinstance(segment_paths, str):
                segment_paths = [segment_paths]

            # Create OR conditions for segment paths
            segment_conditions = []
            for segment_path in segment_paths:
                try:
                    date_str, segment_num_str = segment_path.split('_')
                    segment_num = int(segment_num_str)

                    # Create AND condition for this specific segment
                    segment_condition = {
                        "op": "and",
                        "args": [
                            {
                                "op": "=",
                                "args": [{"property": "opr:date"}, date_str]
                            },
                            {
                                "op": "=",
                                "args": [{"property": "opr:segment"}, segment_num]
                            }
                        ]
                    }
                    segment_conditions.append(segment_condition)
                except ValueError:
                    print(f"Warning: Invalid segment_path format '{segment_path}'. Expected format: YYYYMMDD_NN")
                    continue

            if segment_conditions:
                if len(segment_conditions) == 1:
                    filter_conditions.append(segment_conditions[0])
                else:
                    # Multiple segments - combine with OR
                    filter_conditions.append({
                        "op": "or",
                        "args": segment_conditions
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

        #print(search_params) # TODO: Remove

        # Perform the search
        # from rustac import DuckdbClient
        client = DuckdbClient()
        items = client.search(self.stac_parquet_href, **search_params)
        if isinstance(items, dict):
            items = items['features']

        if not items or len(items) == 0:
            warnings.warn("No items found matching the query criteria", UserWarning)
            return None
        
        # Convert to GeoDataFrame
        items_df = gpd.GeoDataFrame(items)
        # Set index
        items_df = items_df.set_index(items_df['id'])
        items_df.index.name = 'stac_item_id'
        # Set the geometry column
        if 'geometry' in items_df.columns and not exclude_geometry:
            items_df = items_df.set_geometry(items_df['geometry'].apply(shapely.geometry.shape))
            items_df.crs = "EPSG:4326"

        # Reorder the columns, leaving any extra columns at the end
        desired_order = ['collection', 'geometry', 'properties', 'assets']
        items_df = items_df[[col for col in desired_order if col in items_df.columns] + list(items_df.columns.difference(desired_order))]

        return items_df

    def load_frames(self, stac_items: gpd.GeoDataFrame,
                    data_product: str = "CSARP_standard",
                    merge_flights: bool = False,
                    skip_errors: bool = False,
                    ) -> Union[list[xr.Dataset], xr.Dataset]:
        """
        Load multiple radar frames from a list of STAC items.

        Parameters
        ----------
        stac_items : gpd.GeoDataFrame
            The STAC items containing asset URLs.
        data_product : str, optional
            The data product to load (default is "CSARP_standard").
        merge_flights : bool, optional
            Whether to merge frames from the same flight (default is False).
        skip_errors : bool, optional
            Whether to skip errors and continue loading other frames (default is False).

        Returns
        -------
        list[xr.Dataset] or xr.Dataset
            List of loaded radar frames as xarray Datasets or a single merged Dataset if there is only one segment and merge_flights is True.
        """
        frames = []

        for idx, item in stac_items.iterrows():
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
            return opr_tools.merge_flights_from_frames(frames)
        else:
            return frames

    def load_frame(self, stac_item, data_product: str = "CSARP_standard") -> xr.Dataset:
        """
        Load a radar frame from a STAC item.

        Parameters
        ----------
        stac_item
            The STAC item containing asset URLs.
        data_product : str, optional
            The data product to load (default is "CSARP_standard").

        Returns
        -------
        xr.Dataset
            The loaded radar frame as an xarray Dataset.
        """
        
        assets = stac_item['assets']
        
        # Get the data asset
        data_asset = assets.get(data_product)
        if not data_asset:
            available_assets = list(assets.keys())
            raise ValueError(f"No {data_product} asset found. Available assets: {available_assets}")
        
        # Get the URL from the asset
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
        match = re.search(r'(\d{4}_\w+_[A-Za-z0-9]+)\/([\w_]+)\/[\d_]+\/[\w]+(\d{8}_\d{2}_\d{3})', url)
        if match:
            collection, data_product, granule = match.groups()
            date, segment_id, frame_id = granule.split('_')
            ds.attrs['collection'] = collection
            ds.attrs['data_product'] = data_product
            ds.attrs['granule'] = granule
            ds.attrs['segment_path'] = f"{date}_{segment_id}"
            ds.attrs['date_str'] = date
            ds.attrs['segment'] = int(segment_id)
            ds.attrs['frame'] = int(frame_id)

            # Load citation information
            result = ops_api.get_segment_metadata(segment_name=ds.attrs['segment_path'], season_name=collection)
            if result:
                if isinstance(result['data'], str):
                    warnings.warn(f"Warning: Unexpected result from ops_api: {result['data']}", UserWarning)
                else:
                    result_data = {}
                    result_data = result['data']
                    if 'metadata' in result_data:
                        result_data = result_data['metadata'] # New style response from OPS API
                        new_api = True
                    else:
                        new_api = False
                    
                    # Flatten lists with single items to just the item, and convert lists with multiple items to sets
                    for key, value in result_data.items():
                        if len(value) == 0:
                            result_data[key] = None
                        elif len(value) == 1:
                            result_data[key] = value[0]
                        elif len(value) > 1:
                            result_data[key] = set(value)

                    # These match on the old style keys from the OPS API for backwards compatibility
                    if new_api:
                        for k, v in result_data.items():
                            if k in ds.attrs:
                                if ds.attrs[k] != v:
                                    warnings.warn(f"Warning: Conflicting value for attribute '{k}': existing value '{ds.attrs[k]}', new value '{v}'", UserWarning)
                            ds.attrs[k] = v
                    else:
                        if 'dois' in result_data:
                            ds.attrs['DOI'] = result_data['dois']
                        if 'rors' in result_data:
                            ds.attrs['Organization'] = result_data['rors']
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

        client = DuckdbClient()
        return client.get_collections(self.stac_parquet_href)

    def get_segments(self, collection_id: str) -> list:
        """
        Get list of available segments within a collection/season.

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
        items = self.query_frames(collections=[collection_id], exclude_geometry=True)
        
        if items is None or len(items) == 0:
            print(f"No items found in collection '{collection_id}'")
            return []

        # Group items by segment (opr:date + opr:segment)
        segments = {}
        for idx, item in items.iterrows():
            properties = item['properties']
            date = properties['opr:date']
            flight_num = properties['opr:segment']
            
            if date and flight_num is not None:
                segment_path = f"{date}_{flight_num:02d}"

                if segment_path not in segments:
                    segments[segment_path] = {
                        'segment_path': segment_path,
                        'date': date,
                        'flight_number': flight_num,
                        'collection': collection_id,
                        'frames': [],
                        'item_count': 0
                    }

                segments[segment_path]['frames'].append(properties.get('opr:frame'))
                segments[segment_path]['item_count'] += 1

        # Sort segments by date and flight number
        segment_list = list(segments.values())
        segment_list.sort(key=lambda x: (x['date'], x['flight_number']))

        return segment_list

    def get_layers_files(self, segment: Union[xr.Dataset, dict], raise_errors=True) -> dict:
        """
        Fetch layers from the CSARP_layers files
        
        Parameters
        ----------
        segment : Union[xr.Dataset, dict]
            The flight information, which can be an xarray Dataset or a dictionary.
        raise_errors : bool, optional
            If True, raise errors when layers cannot be found.

        Returns
        -------
        dict
            A dictionary mapping layer IDs to their corresponding data.
        """
        if isinstance(segment, xr.Dataset):
            # Get collection and segment information from the dataset attributes
            collection = segment.attrs.get('collection')
            segment_path = segment.attrs.get('segment_path')
            if 'frame' in segment.attrs:
                frame = segment.attrs.get('frame')
            else:
                frame = None # Could be multiple frames in the dataset
        else:
            collection = segment['collection']
            segment_path = f"{segment['properties'].get('opr:date')}_{segment['properties'].get('opr:segment'):02d}"
            frame = segment['properties'].get('opr:frame')

        properties = {}
        if frame:
            properties['opr:frame'] = frame

        # Query STAC collection for CSARP_layer files matching this specific segment

        # Get items from this specific segment
        stac_items = self.query_frames(collections=[collection], segment_paths=[segment_path], properties=properties)

        # Filter for items that have CSARP_layer assets
        layer_items = []
        for idx, item in stac_items.iterrows():
            if 'CSARP_layer' in item['assets']:
                layer_items.append(item)
        
        if not layer_items:
            if raise_errors:
                raise ValueError(f"No CSARP_layer files found for segment path {segment_path} in collection {collection}")
            else:
                return {}
        
        # Load each layer file and combine them
        layer_frames = []
        for item in layer_items:
            layer_asset = item['assets']['CSARP_layer']
            if layer_asset and 'href' in layer_asset:
                url = layer_asset['href']
                try:
                    layer_ds = self.load_layers_file(url)
                    layer_frames.append(layer_ds)
                except Exception as e:
                    print(f"Warning: Failed to load layer file {url}: {e}")
                    continue
        
        if not layer_frames:
            if raise_errors:
                raise ValueError(f"No valid CSARP_layer files could be loaded for segment {segment_path} in collection {collection}")
            else:
                return {}
        
        # Concatenate all layer frames along slow_time dimension
        layers_segment = xr.concat(layer_frames, dim='slow_time', combine_attrs='drop_conflicts', data_vars='all')
        layers_segment = layers_segment.sortby('slow_time')

        # Trim to bounds of the original dataset
        layers_segment = self._trim_to_bounds(layers_segment, segment)

        # Split into separate layers by ID
        layers = {}

        layer_ids = np.unique(layers_segment['id'])

        for i, layer_id in enumerate(layer_ids):
            layer_id_int = int(layer_id)
            layer_data = {}

            for var_name, var_data in layers_segment.data_vars.items():
                if 'layer' in var_data.dims:
                    # Select the i-th layer from 2D variables (layer, slow_time)
                    layer_data[var_name] = (['slow_time'], var_data.isel(layer=i).values)
                else:
                    # 1D variables that don't have layer dimension
                    layer_data[var_name] = var_data
            
            # Create coordinates (excluding layer coordinate)
            coords = {k: v for k, v in layers_segment.coords.items() if k != 'layer'}
            
            # Create the layer dataset
            layer_ds = xr.Dataset(layer_data, coords=coords)
            layers[layer_id_int] = layer_ds
        
        return layers

    def _trim_to_bounds(self, ds: xr.Dataset, ref: Union[xr.Dataset, dict]) -> xr.Dataset:
        start_time, end_time = None, None
        if isinstance(ref, xr.Dataset) and 'slow_time' in ref.coords:
            start_time = ref['slow_time'].min()
            end_time = ref['slow_time'].max()
        else:
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

    def get_layers_db(self, flight: Union[xr.Dataset, dict], include_geometry=True, raise_errors=True) -> dict:
        """
        Fetch layer data from the OPS API

        Parameters
        ----------
        flight : Union[xr.Dataset, dict]
            The flight data, which can be an xarray Dataset or a dictionary.
        include_geometry : bool, optional
            If True, include geometry information in the returned layers.

        Returns
        -------
        dict
            A dictionary mapping layer IDs to their corresponding data.
        """

        if isinstance(flight, xr.Dataset):
            # Get collection and flight information from the dataset attributes
            collection = flight.attrs.get('collection')
            segment_path = flight.attrs.get('segment_path')
        else:
            collection = flight['collection']
            segment_path = f"{flight['properties'].get('opr:date')}_{flight['properties'].get('opr:segment'):02d}"

        if 'Antarctica' in collection:
            location = 'antarctic'
        elif 'Greenland' in collection:
            location = 'arctic'
        else:
            raise ValueError("Dataset does not belong to a recognized location (Antarctica or Greenland).")
        
        layer_points = ops_api.get_layer_points(
            segment_name=segment_path,
            season_name=collection,
            location=location,
            include_geometry=include_geometry
        )

        if layer_points['status'] != 1:
            if raise_errors:
                raise ValueError(f"Failed to fetch layer points. Received response with status {layer_points['status']}.")
            else:
                return {}

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

    def get_layers(self, ds : xr.Dataset, source: str = 'auto', include_geometry=True, raise_errors=True) -> dict:
        """
        Fetch layer data for a given flight dataset, either from CSARP_layer files or the OPS Database API.

        Parameters
        ----------
        ds : xr.Dataset
            The flight dataset containing attributes for collection and segment.
        source : str, optional
            The source to fetch layers from: 'auto', 'files', or 'db' (default is 'auto').
        include_geometry : bool, optional
            If True, include geometry information when fetching from the API.\
        raise_errors : bool, optional
            If True, raise errors when layers cannot be found.

        Returns
        -------
        dict
            A dictionary mapping layer IDs to their corresponding data.
        """

        if source == 'auto':
            # Try to get layers from files first
            try:
                layers = self.get_layers_files(ds, raise_errors=True)
                return layers
            except:
                # Fallback to API if no layers found in files
                return self.get_layers_db(ds, include_geometry=include_geometry, raise_errors=raise_errors)
        elif source == 'files':
            return self.get_layers_files(ds, raise_errors=raise_errors)
        elif source == 'db':
            return self.get_layers_db(ds, include_geometry=include_geometry, raise_errors=raise_errors)
        else:
            raise ValueError("Invalid source specified. Must be one of: 'auto', 'files', 'db'.")
