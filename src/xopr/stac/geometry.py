"""
Geometry operations for STAC catalog creation.

This module contains functions for handling spatial geometries including
simplification, merging, and projection transformations for polar data.
"""

from typing import List, Dict, Any, Optional
import shapely
from shapely.geometry import mapping, LineString
import pyproj
from shapely.ops import transform
import pystac


def simplify_geometry_polar_projection(
    geometry: shapely.geometry.base.BaseGeometry, 
    simplify_tolerance: float = 100.0
) -> shapely.geometry.base.BaseGeometry:
    """
    Simplify geometry using appropriate polar stereographic projection.
    
    Parameters
    ----------
    geometry : shapely.geometry.base.BaseGeometry
        Input geometry in WGS84 coordinates
    simplify_tolerance : float, default 100.0
        Tolerance for shapely.simplify() in meters (used in polar projection)
        
    Returns
    -------
    shapely.geometry.base.BaseGeometry
        Simplified geometry in WGS84 coordinates
    """
    if not geometry or not geometry.is_valid:
        return geometry
    
    # Determine appropriate polar projection based on geometry centroid
    centroid = geometry.centroid
    lat = centroid.y
    
    if lat < 0:
        # Antarctic/South Polar Stereographic
        target_epsg = 3031
    else:
        # Arctic/North Polar Stereographic  
        target_epsg = 3413
    
    # Set up coordinate transformations
    wgs84 = pyproj.CRS('EPSG:4326')
    polar_proj = pyproj.CRS(f'EPSG:{target_epsg}')
    
    # Transform to polar projection
    transformer_to_polar = pyproj.Transformer.from_crs(wgs84, polar_proj, always_xy=True)
    transformer_to_wgs84 = pyproj.Transformer.from_crs(polar_proj, wgs84, always_xy=True)
    
    # Project to polar coordinates
    projected_geom = transform(transformer_to_polar.transform, geometry)
    
    # Simplify in projected coordinates (tolerance in meters)
    simplified_geom = projected_geom.simplify(simplify_tolerance, preserve_topology=True)
    
    # Transform back to WGS84
    return transform(transformer_to_wgs84.transform, simplified_geom)


def merge_item_geometries(
    items: List[pystac.Item], 
    simplify_tolerance: float = 100.0
) -> Optional[Dict[str, Any]]:
    """
    Merge geometries from multiple STAC items into a single simplified geometry.
    
    For flight data with segments, creates a connected LineString by concatenating
    coordinates in segment order. For other data, performs geometric union.
    Geometries are projected to appropriate polar stereographic projections 
    before merging and simplification to handle longitude wrap-around issues near the poles.
    
    Parameters
    ----------
    items : list of pystac.Item
        List of STAC items to merge geometries from.
    simplify_tolerance : float, default 100.0
        Tolerance for shapely.simplify() in meters (used in polar projection).
        
    Returns
    -------
    dict or None
        GeoJSON geometry object representing the simplified connected LineString (for flight data)
        or union geometry (for other data), or None if no valid geometries found.
    """
    if not items:
        return None
    
    # Check if all items have frame information for flight data
    has_frames = all(
        item.properties and item.properties.get('opr:frame') is not None
        for item in items
    )

    if has_frames:
        # Flight data: sort by frame and concatenate coordinates
        items_with_geoms = []
        for item in items:
            if item.geometry:
                try:
                    geom = shapely.geometry.shape(item.geometry)
                    if geom.is_valid and geom.geom_type == 'LineString':
                        frame_num = item.properties.get('opr:frame')
                        items_with_geoms.append((frame_num, geom, item))
                except Exception:
                    continue
        
        if not items_with_geoms:
            return None
        
        # Sort by frame number
        items_with_geoms.sort(key=lambda x: x[0])

        # Concatenate coordinates from all LineStrings in order
        all_coords = []
        for frame_num, geom, item in items_with_geoms:
            coords = list(geom.coords)
            if all_coords and coords:
                # Skip first coordinate if it's the same as the last coordinate
                # (to avoid duplicate points at frame boundaries)
                if all_coords[-1] == coords[0]:
                    coords = coords[1:]
            all_coords.extend(coords)
        
        if len(all_coords) < 2:
            return None
        
        # Create connected LineString
        connected_linestring = LineString(all_coords)
        
    else:
        # Non-flight data: use geometric union approach
        geometries = []
        for item in items:
            if item.geometry:
                try:
                    geom = shapely.geometry.shape(item.geometry)
                    if geom.is_valid:
                        geometries.append(geom)
                except Exception:
                    continue
        
        if not geometries:
            return None
        
        # Union all geometries in lat/lon first
        union_geom = geometries[0]
        for geom in geometries[1:]:
            union_geom = union_geom.union(geom)
        
        connected_linestring = union_geom
    
    # Simplify using unified polar projection function
    simplified_geom = simplify_geometry_polar_projection(connected_linestring, simplify_tolerance)
    
    # Convert back to GeoJSON
    return mapping(simplified_geom)


def merge_flight_geometries(flight_geometries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Merge simplified flight geometries into a single MultiLineString.
    
    This function is designed for campaign-level collections where individual
    flight geometries need to be combined into a MultiLineString since they
    are not spatially continuous.
    
    Parameters
    ----------
    flight_geometries : list of dict
        List of GeoJSON geometry objects (can be LineStrings or MultiLineStrings) from
        flight collections that have already been simplified.
        
    Returns
    -------
    dict or None
        GeoJSON MultiLineString geometry object containing all flight geometries,
        or None if no valid geometries found. If there's only one LineString,
        returns it as-is without wrapping in MultiLineString.
    """
    if not flight_geometries:
        return None
    
    all_linestrings = []
    for geom_dict in flight_geometries:
        if geom_dict is None:
            continue
            
        try:
            geom = shapely.geometry.shape(geom_dict)
            if geom.is_valid:
                # Handle both LineString and MultiLineString geometries
                if geom.geom_type == 'LineString':
                    all_linestrings.append(geom)
                elif geom.geom_type == 'MultiLineString':
                    # Extract individual LineStrings from MultiLineString
                    all_linestrings.extend(list(geom.geoms))
        except Exception:
            continue
    
    if not all_linestrings:
        return None
    
    # Create MultiLineString from all linestrings
    if len(all_linestrings) == 1:
        # If only one linestring, return it as-is (not wrapped in MultiLineString)
        multi_linestring = all_linestrings[0]
    else:
        multi_linestring = shapely.geometry.MultiLineString(all_linestrings)
    
    # Convert back to GeoJSON
    return mapping(multi_linestring)


def build_collection_extent_and_geometry(
    items: List[pystac.Item]
) -> tuple[pystac.Extent, Optional[Dict[str, Any]]]:
    """
    Calculate spatial and temporal extent from a list of items, plus merged geometry.
    
    Parameters
    ----------
    items : list of pystac.Item
        List of STAC items to compute extent from.
        
    Returns
    -------
    tuple of (pystac.Extent, dict or None)
        Combined spatial and temporal extent covering all input items,
        and merged geometry as GeoJSON dict (or None if no geometries).
        
    Raises
    ------
    ValueError
        If items list is empty.
    """
    if not items:
        raise ValueError("Cannot build extent from empty item list")
    
    # Merge actual geometries for proj:geometry
    merged_geometry = merge_item_geometries(items)
    
    # Build extent using bboxes (existing logic)
    bboxes = []
    datetimes = []
    
    for item in items:
        if item.bbox:
            bbox_geom = shapely.geometry.box(*item.bbox)
            bboxes.append(bbox_geom)
        
        if item.datetime:
            datetimes.append(item.datetime)
    
    if bboxes:
        union_bbox = bboxes[0]
        for bbox in bboxes[1:]:
            union_bbox = union_bbox.union(bbox)
        
        collection_bbox = list(union_bbox.bounds)
        spatial_extent = pystac.SpatialExtent(bboxes=[collection_bbox])
    else:
        spatial_extent = pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]])
    
    if datetimes:
        sorted_times = sorted(datetimes)
        temporal_extent = pystac.TemporalExtent(
            intervals=[[sorted_times[0], sorted_times[-1]]]
        )
    else:
        temporal_extent = pystac.TemporalExtent(intervals=[[None, None]])
    
    extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)
    return extent, merged_geometry


def build_collection_extent(items: List[pystac.Item]) -> pystac.Extent:
    """
    Calculate spatial and temporal extent from a list of items.
    
    Parameters
    ----------
    items : list of pystac.Item
        List of STAC items to compute extent from.
        
    Returns
    -------
    pystac.Extent
        Combined spatial and temporal extent covering all input items.
        
    Raises
    ------
    ValueError
        If items list is empty.
    """
    if not items:
        raise ValueError("Cannot build extent from empty item list")
    
    bboxes = []
    datetimes = []
    
    for item in items:
        if item.bbox:
            bbox_geom = shapely.geometry.box(*item.bbox)
            bboxes.append(bbox_geom)
        
        if item.datetime:
            datetimes.append(item.datetime)
    
    if bboxes:
        union_bbox = bboxes[0]
        for bbox in bboxes[1:]:
            union_bbox = union_bbox.union(bbox)
        
        collection_bbox = list(union_bbox.bounds)
        spatial_extent = pystac.SpatialExtent(bboxes=[collection_bbox])
    else:
        spatial_extent = pystac.SpatialExtent(bboxes=[[-180, -90, 180, 90]])
    
    if datetimes:
        sorted_times = sorted(datetimes)
        temporal_extent = pystac.TemporalExtent(
            intervals=[[sorted_times[0], sorted_times[-1]]]
        )
    else:
        temporal_extent = pystac.TemporalExtent(intervals=[[None, None]])
    
    return pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)