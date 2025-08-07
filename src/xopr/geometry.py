import geopandas as gpd
import json
from cartopy import crs
from pyproj import Transformer
import shapely.ops


def get_antarctic_regions(
    name=None,
    regions=None, 
    subregions=None,
    type=None,
    merge_regions=True,
    measures_boundaries_url = "https://storage.googleapis.com/opr_stac/reference_geometry/measures_boundaries_4326.geojson"
):
    """
    Load and filter Antarctic regional boundaries from the MEASURES dataset.
    
    Parameters
    ----------
    name : str or list, optional
        NAME field value(s) to filter by
    regions : str or list, optional
        REGIONS field value(s) to filter by  
    subregions : str or list, optional
        SUBREGION field value(s) to filter by
    type : str or list, optional
        TYPE field value(s) to filter by
    merge_regions : bool, default True
        If True, return a single merged geometry; if False, return list of geometries
    measures_boundaries_url : str, default "https://storage.googleapis.com/opr_stac/reference_geometry/measures_boundaries_4326.geojson"
        URL to the GeoJSON file containing Antarctic region boundaries
        
    Returns
    -------
    list or dict
        If merge_regions=False: List of GeoJSON geometry dicts
        If merge_regions=True: Single GeoJSON geometry dict of merged regions
        
    Examples
    --------
    # Get George VI ice shelf
    >>> george_vi = get_antarctic_regions(name="George_VI", type="FL")
    
    # Get all ice shelves, merged into one geometry
    >>> all_shelves = get_antarctic_regions(type="FL", merge_regions=True)
    
    # Get multiple regions by name
    >>> regions = get_antarctic_regions(name=["George_VI", "LarsenC"])
    """
    
    
    # Load the boundaries GeoJSON from the reference URL
    filtered = gpd.read_file(measures_boundaries_url)
    
    # Apply filters based on provided parameters
    if name is not None:
        if isinstance(name, str):
            name = [name]
        filtered = filtered[filtered['NAME'].isin(name)]
    
    if regions is not None:
        if isinstance(regions, str):
            regions = [regions] 
        filtered = filtered[filtered['Regions'].isin(regions)]
        
    if subregions is not None:
        if isinstance(subregions, str):
            subregions = [subregions]
        filtered = filtered[filtered['Subregions'].isin(subregions)]
        
    if type is not None:
        if isinstance(type, str):
            type = [type]
        filtered = filtered[filtered['TYPE'].isin(type)]

    # Filter out invalid geometries
    filtered = filtered[filtered.is_valid]
    
    if len(filtered) == 0:
        return [] if not merge_regions else None
    
    if merge_regions:
        merged = filtered.iloc[0].geometry
        for i in range(1, len(filtered)):
            merged = merged.union(filtered.iloc[i].geometry)
        return merged
    else:
        return filtered
    
def project_dataset(ds, target_crs):
    projected_coords = target_crs.transform_points(
        crs.PlateCarree(), ds['Longitude'].values, ds['Latitude'].values
    ).T
    ds = ds.assign_coords({
        'x': (('slow_time'), projected_coords[0]),
        'y': (('slow_time'), projected_coords[1])
    })
    return ds

def project_geojson(geometry, source_crs="EPSG:4326", target_crs="EPSG:3031"):
    transformer = Transformer.from_crs(source_crs, target_crs, always_xy=True)
    projected_geometry = shapely.ops.transform(transformer.transform, geometry)
    return projected_geometry