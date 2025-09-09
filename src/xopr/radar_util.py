import xarray as xr
import shapely
import numpy as np
import scipy.constants

from xopr.geometry import project_dataset

def add_along_track_coordinate(ds: xr.Dataset, projection: str = None) -> xr.Dataset:
    """
    Add an along-track distance coordinate to the dataset based on the latitude and longitude coordinates.
    
    Parameters:
    ds (xr.Dataset): Input xarray Dataset with 'Latitude' and 'Longitude' coordinates.
    
    Returns:
    xr.Dataset: Dataset with an added 'along_track' coordinate.
    """

    if 'Latitude' not in ds or 'Longitude' not in ds:
        raise ValueError("Dataset must contain 'Latitude' and 'Longitude' coordinates.")
    
    # Project the dataset to the specified projection
    if projection is None:
        if ds['Latitude'].mean() < 0:
            projection = "EPSG:3031"  # Antarctic Polar Stereographic
        else:
            projection = "EPSG:3413"  # Arctic Polar Stereographic
    projected_ds = project_dataset(ds, target_crs=projection)

    # Calculate differences between consecutive points
    dx = projected_ds['x'].diff(dim='slow_time', label='upper').to_numpy()
    dy = projected_ds['y'].diff(dim='slow_time', label='upper').to_numpy()

    # Calculate incremental distances
    distances = (dx**2 + dy**2)**0.5
    # Add a zero at the start to align with slow_time
    distances = np.insert(distances, 0, 0)
    
    # Calculate cumulative distance along track
    along_track = np.cumsum(distances)
    
    # Add the along-track coordinate to the original dataset
    ds = ds.assign_coords(along_track=('slow_time', along_track))
    ds['along_track'].attrs['units'] = 'meters'
    ds['along_track'].attrs['description'] = 'Cumulative distance along the radar track'

    return ds

def estimate_vertical_distances(ds: xr.Dataset, epsilon_ice: float = 3.15) -> xr.Dataset:
    """
    Estimate vertical distances from two-way travel time (TWTT) using the speed of light in ice.
    
    Parameters:
    ds (xr.Dataset): Input xarray Dataset with TWTT layers as variables.
    epsilon_ice (float): Relative permittivity of ice. Default is 3.15.
    
    Returns:
    xr.Dataset: Dataset with added vertical distance variables for each TWTT layer.
    """
    
    v_ice = scipy.constants.c / np.sqrt(epsilon_ice)  # Speed of light in ice (m/s)
    
    # Initialize local_speed with dimensions (slow_time, twtt) to always be scipy.constants.c
    local_speed = xr.full_like(ds['Data'], scipy.constants.c)
    
    # Where twtt (a 1D dimension) > ds['Surface'] (data variable with dimension slow_time), set local_speed to v_ice
    # Broadcast comparison: expand Surface to match Data dimensions
    surface_broadcast = ds['Surface'].broadcast_like(ds['Data'])
    twtt_broadcast = ds['twtt'].broadcast_like(ds['Data'])
    local_speed = xr.where(twtt_broadcast > surface_broadcast, v_ice, scipy.constants.c)
    
    # Multiply against the differences in the twtt dimension to get the distance intervals
    twtt_intervals = np.diff(ds['twtt'])
    twtt_intervals = np.insert(twtt_intervals, 0, twtt_intervals[0])  # Add the first interval
    twtt_intervals = xr.DataArray(twtt_intervals, dims=['twtt'], coords={'twtt': ds['twtt']})

    # Calculate distance for each interval (one-way distance = speed * time / 2)
    distance_intervals = local_speed * twtt_intervals / 2
    
    # Cumulatively sum the distance intervals to get the vertical distance
    vertical_distance = distance_intervals.cumsum(dim='twtt')
    vertical_distance.name = 'vertical_distance'
    vertical_distance.attrs['units'] = 'meters'
    vertical_distance.attrs['description'] = 'Vertical distance from aircraft calculated from TWTT'
    
    return vertical_distance


def interpolate_to_vertical_grid(ds: xr.Dataset, 
                                  vertical_coordinate: str = 'range',
                                  vert_min: float = None, 
                                  vert_max: float = None, 
                                  vert_spacing: float = 10.0,
                                  epsilon_ice: float = 3.15) -> xr.Dataset:
    """
    Interpolate radar data from TWTT coordinates to regular vertical distance coordinates.
    
    Parameters:
    -----------
    ds : xr.Dataset
        Input dataset with 'Data' variable, 'along_track' coordinate, and 'Surface' variable
    vertical_coordinate : str
        The vertical coordinate to use for interpolation. 'range' will interpolate to the
        vertical range from the instrument. 'wgs84' will interpolate to WGS84 elevation
        using the 'Elevation' variable in the dataset. Default is 'range'.
    vert_min : float
        Minimum vertical distance in meters, if None, uses minimum from data
    vert_max : float
        Maximum vertical distance in meters, if None, uses maximum from data
    vert_spacing : float
        Vertical spacing in meters
    epsilon_ice : float
        Relative permittivity of ice (default 3.15)
    
    Returns:
    --------
    xr.Dataset
        Dataset with data interpolated to regular vertical distance grid
    """
    from scipy.interpolate import griddata
    
    # Calculate vertical distances
    vert_dist = estimate_vertical_distances(ds, epsilon_ice)

    vert_coord_name = 'range'

    if vertical_coordinate == 'wgs84':
        if 'Elevation' not in ds:
            raise ValueError("Dataset must contain 'Elevation' variable to use elevation as vertical coordinate.")
        vert_dist = ds['Elevation'].broadcast_like(vert_dist) - vert_dist
        vert_coord_name = 'wgs84'
    elif vertical_coordinate != 'range':
        raise ValueError("vertical_coordinate must be either 'range' or 'wgs84'")

    if vert_min is None:
        vert_min = float(vert_dist.min().values)
    if vert_max is None:
        vert_max = float(vert_dist.max().values)
    
    # Create regular vertical distance grid
    regular_vert = np.arange(vert_min, vert_max, vert_spacing)
    
    # Get along-track values
    along_track = ds['along_track'].values
    
    # Use 1D interpolation along each trace (much faster than 2D griddata)
    from scipy.interpolate import interp1d
    
    n_traces = ds.sizes['slow_time']
    n_vert = len(regular_vert)
    data_regular = np.full((n_traces, n_vert), np.nan, dtype=np.float32)
    
    # Interpolate each trace individually
    for i in range(n_traces):
        trace_data = ds['Data'].isel(slow_time=i).values
        trace_vert = vert_dist.isel(slow_time=i).values
        
        # Remove NaN values for this trace
        valid_idx = ~(np.isnan(trace_data) | np.isnan(trace_vert))
        
        if np.sum(valid_idx) > 1:  # Need at least 2 points for interpolation
            # Sort by vertical distance for interpolation
            sort_idx = np.argsort(trace_vert[valid_idx])
            vert_sorted = trace_vert[valid_idx][sort_idx]
            data_sorted = trace_data[valid_idx][sort_idx]
            
            # Only interpolate within the range of available data
            vert_range = (regular_vert >= vert_sorted.min()) & (regular_vert <= vert_sorted.max())
            
            if np.sum(vert_range) > 0:
                # Use linear interpolation (much faster than griddata)
                interp_func = interp1d(vert_sorted, data_sorted, kind='linear', 
                                     bounds_error=False, fill_value=np.nan)
                data_regular[i, vert_range] = interp_func(regular_vert[vert_range])
    
    # Create new dataset
    ds_regular = xr.Dataset(
        {
            'Data': (('slow_time', vert_coord_name), data_regular),
        },
        coords={
            'slow_time': ds['slow_time'],
            'along_track': ('slow_time', along_track),
            vert_coord_name: regular_vert,
        }
    )

    for data_var in ds.data_vars:
        if data_var not in ['Data']:
            ds_regular[data_var] = ds[data_var]
    
    # Copy relevant attributes
    ds_regular.attrs = ds.attrs.copy()
    ds_regular[vert_coord_name].attrs['units'] = 'meters'
    if vertical_coordinate == 'range':
        ds_regular[vert_coord_name].attrs['description'] = 'Vertical distance from aircraft (positive down)'
    else:
        ds_regular[vert_coord_name].attrs['description'] = 'WGS84 Elevation (meters)'

    return ds_regular