import xarray as xr
import shapely
import numpy as np

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
