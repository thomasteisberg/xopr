"""
STAC catalog creation utilities for Open Polar Radar data.
"""

import re
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

import numpy as np
import pystac
import shapely
from shapely.geometry import mapping, LineString
import pyproj
from shapely.ops import transform

from .metadata import extract_item_metadata, discover_campaigns, discover_flight_lines

# STAC extension URLs
SCI_EXT = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
SAR_EXT = 'https://stac-extensions.github.io/sar/v1.3.0/schema.json'
PROJ_EXT = 'https://stac-extensions.github.io/projection/v2.0.0/schema.json'


def create_catalog(
    catalog_id: str = "OPR",
    description: str = "Open Polar Radar airborne data",
    stac_extensions: Optional[List[str]] = None
) -> pystac.Catalog:
    """
    Create a root STAC catalog for OPR data.
    
    Parameters
    ----------
    catalog_id : str, default "OPR"
        Unique identifier for the catalog.
    description : str, default "Open Polar Radar airborne data"
        Human-readable description of the catalog.
    stac_extensions : list of str, optional
        List of STAC extension URLs to enable. If None, defaults to
        projection and file extensions.
        
    Returns
    -------
    pystac.Catalog
        Root catalog object.
    """
    if stac_extensions is None:
        stac_extensions = [
            'https://stac-extensions.github.io/projection/v2.0.0/schema.json',
            'https://stac-extensions.github.io/file/v2.1.0/schema.json',
        ]
    
    return pystac.Catalog(
        id=catalog_id,
        description=description,
        stac_extensions=stac_extensions
    )


def create_collection(
    collection_id: str,
    description: str,
    extent: pystac.Extent,
    license: str = "various",
    stac_extensions: Optional[List[str]] = None,
    geometry: Optional[Dict[str, Any]] = None
) -> pystac.Collection:
    """
    Create a STAC collection for a campaign or data product grouping.
    
    Parameters
    ----------
    collection_id : str
        Unique identifier for the collection.
    description : str
        Human-readable description of the collection.
    extent : pystac.Extent
        Spatial and temporal extent of the collection.
    license : str, default ""
        Data license identifier.
    stac_extensions : list of str, optional
        List of STAC extension URLs to enable. If None, defaults to
        empty list.
    geometry : dict, optional
        GeoJSON geometry object for the collection. If provided, the
        projection extension will be added automatically.
        
    Returns
    -------
    pystac.Collection
        Collection object.
    """
    if stac_extensions is None:
        stac_extensions = []
    
    # Add projection extension if geometry is provided
    if geometry is not None and PROJ_EXT not in stac_extensions:
        stac_extensions = stac_extensions + [PROJ_EXT]
    
    collection = pystac.Collection(
        id=collection_id,
        description=description,
        extent=extent,
        license=license,
        stac_extensions=stac_extensions
    )
    
    # Add geometry to extra_fields if provided
    if geometry is not None:
        collection.extra_fields['proj:geometry'] = geometry
    
    return collection


def create_item(
    item_id: str,
    geometry: Dict[str, Any],
    bbox: List[float],
    datetime: Any,
    properties: Optional[Dict[str, Any]] = None,
    assets: Optional[Dict[str, pystac.Asset]] = None,
    stac_extensions: Optional[List[str]] = None
) -> pystac.Item:
    """
    Create a STAC item for a flight line data segment.
    
    Parameters
    ----------
    item_id : str
        Unique identifier for the item.
    geometry : dict
        GeoJSON geometry object.
    bbox : list of float
        Bounding box coordinates [xmin, ymin, xmax, ymax].
    datetime : datetime
        Acquisition datetime.
    properties : dict, optional
        Additional metadata properties. If None, defaults to empty dict.
    assets : dict of str to pystac.Asset, optional
        Dictionary of assets (data files, thumbnails, etc.). Keys are 
        asset names, values are pystac.Asset objects.
    stac_extensions : list of str, optional
        List of STAC extension URLs to enable. If None, defaults to
        file extension.
        
    Returns
    -------
    pystac.Item
        Item object with specified properties and assets.
    """
    if properties is None:
        properties = {}
    if stac_extensions is None:
        stac_extensions = ['https://stac-extensions.github.io/file/v2.1.0/schema.json']
    
    item = pystac.Item(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime,
        properties=properties,
        stac_extensions=stac_extensions
    )
    
    if assets:
        for key, asset in assets.items():
            item.add_asset(key, asset)
    
    return item


def create_items_from_flight_data(
    flight_data: Dict[str, Any],
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    campaign_name: str = "",
    primary_data_product: str = "CSARP_standard",
    verbose: bool = False
) -> List[pystac.Item]:
    """
    Create STAC items from flight line data.
    
    Parameters
    ----------
    flight_data : dict
        Flight metadata from discover_flight_lines(). Expected to contain
        'flight_id' and 'data_files' keys.
    base_url : str, default "https://data.cresis.ku.edu/data/rds/"
        Base URL for constructing asset hrefs.
    campaign_name : str, default ""
        Campaign name for URL construction.
    primary_data_product : str, default "CSARP_standard"
        Data product name to use as primary data source.
    verbose : bool, default False
        If True, print details for each item being processed.
        
    Returns
    -------
    list of pystac.Item
        List of STAC Item objects, one per MAT file in the flight data.
        Each item contains geometry, temporal information, and asset links.
    """
    items = []
    flight_id = flight_data['flight_id']

    primary_data_files = flight_data['data_files'][primary_data_product].values()
    
    for data_file_path in primary_data_files:
        data_path = Path(data_file_path)
        
        try:
            # Extract metadata from MAT file only (no CSV needed)
            metadata = extract_item_metadata(data_path)
        except Exception as e:
            print(f"Warning: Failed to extract metadata for {data_path}: {e}")
            continue

        item_id = f"{data_path.stem}"
        
        geometry = mapping(metadata['geom'])
        bbox = list(metadata['bbox'].bounds)
        datetime = metadata['date']

        rel_mat_path = f"{campaign_name}/{primary_data_product}/{flight_id}/{data_path.name}"
        data_href = base_url + rel_mat_path
        
        # Extract segment number from MAT filename (e.g., "Data_20161014_03_001.mat" -> "001")
        segment_match = re.search(r'_(\d+)\.mat$', data_path.name)
        segment = segment_match.group(1)
        
        # Extract date and flight number from flight_id (e.g., "20161014_03" -> "20161014", "03")
        # Split on underscore to avoid assuming fixed lengths
        parts = flight_id.split('_')
        date_part = parts[0]  # YYYYMMDD
        flight_num_str = parts[1]  # Flight number as string
        
        # Create OPR-specific properties
        properties = {
            'opr:date': date_part,
            'opr:flight': int(flight_num_str),
            'opr:segment': int(segment)
        }
        
        # Add scientific extension properties if available
        item_stac_extensions = ['https://stac-extensions.github.io/file/v2.1.0/schema.json']
        
        if metadata.get('doi') is not None:
            properties['sci:doi'] = metadata['doi']
        
        if metadata.get('citation') is not None:
            properties['sci:citation'] = metadata['citation']
        
        if metadata.get('doi') is not None or metadata.get('citation') is not None:
            item_stac_extensions.append('https://stac-extensions.github.io/scientific/v1.0.0/schema.json')
        
        # Add SAR extension properties if available
        if metadata.get('frequency') is not None:
            properties['sar:center_frequency'] = metadata['frequency']
        
        if metadata.get('bandwidth') is not None:
            properties['sar:bandwidth'] = metadata['bandwidth']
        
        if metadata.get('frequency') is not None or metadata.get('bandwidth') is not None:
            item_stac_extensions.append('https://stac-extensions.github.io/sar/v1.3.0/schema.json')
        
        assets = {}

        for data_product_type in flight_data['data_files'].keys():
            if data_path.name in flight_data['data_files'][data_product_type]:
                product_path = flight_data['data_files'][data_product_type][data_path.name]
                file_type = metadata.get('mimetype') # get_mat_file_type(product_path)
                if verbose:
                    print(f"[{file_type}] {product_path}")
                assets[data_product_type] = pystac.Asset(
                    href=base_url + f"{campaign_name}/{data_product_type}/{flight_id}/{data_path.name}",
                    media_type=file_type
                )
                if data_product_type == primary_data_product:
                    assets['data'] = assets[data_product_type]
        
        thumb_href = base_url + f"{campaign_name}/images/{flight_id}/{flight_id}_{segment}_2echo_picks.jpg"
        assets['thumbnails'] = pystac.Asset(
            href=thumb_href,
            media_type=pystac.MediaType.JPEG
        )
        
        flight_path_href = base_url + f"{campaign_name}/images/{flight_id}/{flight_id}_{segment}_0maps.jpg"
        assets['flight_path'] = pystac.Asset(
            href=flight_path_href,
            media_type=pystac.MediaType.JPEG
        )
        
        item = create_item(
            item_id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=datetime,
            properties=properties,
            assets=assets,
            stac_extensions=item_stac_extensions
        )
        
        items.append(item)
    
    return items


def simplify_geometry_polar_projection(geometry: shapely.geometry.base.BaseGeometry, 
                                      simplify_tolerance: float = 100.0) -> shapely.geometry.base.BaseGeometry:
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


def merge_item_geometries(items: List[pystac.Item], simplify_tolerance: float = 100.0) -> Optional[Dict[str, Any]]:
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
    
    # Check if all items have segment information for flight data
    has_segments = all(
        item.properties and item.properties.get('opr:segment') is not None 
        for item in items
    )
    
    if has_segments:
        # Flight data: sort by segment and concatenate coordinates
        items_with_geoms = []
        for item in items:
            if item.geometry:
                try:
                    geom = shapely.geometry.shape(item.geometry)
                    if geom.is_valid and geom.geom_type == 'LineString':
                        segment_num = item.properties.get('opr:segment')
                        items_with_geoms.append((segment_num, geom, item))
                except Exception:
                    continue
        
        if not items_with_geoms:
            return None
        
        # Sort by segment number
        items_with_geoms.sort(key=lambda x: x[0])
        
        # Concatenate coordinates from all LineStrings in order
        all_coords = []
        for segment_num, geom, item in items_with_geoms:
            coords = list(geom.coords)
            if all_coords and coords:
                # Skip first coordinate if it's the same as the last coordinate
                # (to avoid duplicate points at segment boundaries)
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


def build_collection_extent_and_geometry(items: List[pystac.Item]) -> tuple[pystac.Extent, Optional[Dict[str, Any]]]:
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


def build_limited_catalog(
    data_root: Path,
    output_path: Path,
    catalog_id: str = "OPR",
    data_product: str = "CSARP_standard",
    extra_data_products: list[str] = ['CSARP_layer',
                                      'CSARP_qlook'],
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_items: int = 10,
    campaign_filter: list = None,
    verbose: bool = False
) -> pystac.Catalog:
    """
    Build STAC catalog with limits for faster processing.

    Parameters
    ----------
    data_root : Path
        Root directory containing campaign data
    output_path : Path
        Directory where catalog will be saved
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    data_product : str, optional
        Primary data product to process, by default "CSARP_standard"
    extra_data_products : List[str], optional
        Additional data products to include, by default ['CSARP_layer', 'CSARP_qlook']
    base_url : str, optional
        Base URL for asset hrefs, by default "https://data.cresis.ku.edu/data/rds/"
    max_items : int, optional
        Maximum number of items to process, by default 10
    campaign_filter : List[str], optional
        Specific campaigns to process, by default None (all campaigns)
    verbose : bool, optional
        If True, print details for each item being processed, by default False

    Returns
    -------
    pystac.Catalog
        The built STAC catalog
    """
    catalog = create_catalog(catalog_id=catalog_id)
    campaigns = discover_campaigns(data_root)

    # Filter campaigns if specified
    if campaign_filter:
        campaigns = [c for c in campaigns if c['name'] in campaign_filter]

    for campaign in campaigns:
        campaign_path = Path(campaign['path'])
        campaign_name = campaign['name']

        print(f"Processing campaign: {campaign_name}")

        try:
            flight_lines = discover_flight_lines(
                campaign_path, data_product,
                extra_data_products=extra_data_products
            )
        except FileNotFoundError as e:
            print(f"Warning: Skipping {campaign_name}: {e}")
            continue

        if not flight_lines:
            print(f"Warning: No flight lines found for {campaign_name}")
            continue

        # Limit the number of flight lines processed if specified
        if max_items is not None:
            flight_lines = flight_lines[:max_items]

        # Group items by flight_id to create flight collections
        flight_collections = []
        all_campaign_items = []
        flight_geometries = []

        for flight_data in flight_lines:
            try:
                items = create_items_from_flight_data(
                    flight_data, base_url, campaign_name, data_product, verbose
                )

                # Create flight collection
                flight_id = flight_data['flight_id']
                flight_extent, flight_geometry = build_collection_extent_and_geometry(items)

                # Collect scientific metadata from items for flight collection
                dois = [
                    item.properties.get('sci:doi') for item in items
                    if item.properties.get('sci:doi') is not None
                ]
                citations = [
                    item.properties.get('sci:citation') for item in items
                    if item.properties.get('sci:citation') is not None
                ]

                # Check for unique values and prepare extensions
                flight_extensions = []
                flight_extra_fields = {}

                if dois and len(np.unique(dois)) == 1:
                    flight_extensions.append(SCI_EXT)
                    flight_extra_fields['sci:doi'] = dois[0]

                if citations and len(np.unique(citations)) == 1:
                    if SCI_EXT not in flight_extensions:
                        flight_extensions.append(SCI_EXT)
                    flight_extra_fields['sci:citation'] = citations[0]

                # Collect SAR metadata from items for flight collection
                center_frequencies = [
                    item.properties.get('sar:center_frequency')
                    for item in items
                    if item.properties.get('sar:center_frequency') is not None
                ]
                bandwidths = [
                    item.properties.get('sar:bandwidth')
                    for item in items
                    if item.properties.get('sar:bandwidth') is not None
                ]

                if (center_frequencies and
                        len(np.unique(center_frequencies)) == 1):
                    flight_extensions.append(SAR_EXT)
                    flight_extra_fields['sar:center_frequency'] = (
                        center_frequencies[0]
                    )

                if bandwidths and len(np.unique(bandwidths)) == 1:
                    if SAR_EXT not in flight_extensions:
                        flight_extensions.append(SAR_EXT)
                    flight_extra_fields['sar:bandwidth'] = bandwidths[0]

                flight_collection = create_collection(
                    collection_id=flight_id,
                    description=(
                        f"Flight {flight_id} data from {campaign['year']} "
                        f"{campaign['aircraft']} over {campaign['location']}"
                    ),
                    extent=flight_extent,
                    stac_extensions=(
                        flight_extensions if flight_extensions else None
                    ),
                    geometry=flight_geometry
                )

                # Add scientific extra fields to flight collection
                for key, value in flight_extra_fields.items():
                    flight_collection.extra_fields[key] = value

                # Add items to flight collection
                flight_collection.add_items(items)
                flight_collections.append(flight_collection)
                all_campaign_items.extend(items)
                
                # Collect flight geometry for campaign-level merging
                if flight_geometry is not None:
                    flight_geometries.append(flight_geometry)

                print(
                    f"  Added flight collection {flight_id} with "
                    f"{len(items)} items"
                )

                # Break early if we've hit the max items limit
                if (max_items is not None and
                        len(all_campaign_items) >= max_items):
                    break

            except Exception as e:
                flight_id = flight_data['flight_id']
                print(f"Warning: Failed to process flight {flight_id}: {e}")
                continue

        if flight_collections:
            # Create campaign collection with extent covering all flights
            campaign_extent = build_collection_extent(all_campaign_items)
            
            # Merge flight geometries into campaign-level MultiLineString
            campaign_geometry = merge_flight_geometries(flight_geometries)

            # Collect scientific metadata from all campaign items
            campaign_dois = [
                item.properties.get('sci:doi') for item in all_campaign_items
                if item.properties.get('sci:doi') is not None
            ]
            campaign_citations = [
                item.properties.get('sci:citation')
                for item in all_campaign_items
                if item.properties.get('sci:citation') is not None
            ]

            # Check for unique values and prepare extensions
            campaign_extensions = []
            campaign_extra_fields = {}

            if campaign_dois and len(np.unique(campaign_dois)) == 1:
                campaign_extensions.append(SCI_EXT)
                campaign_extra_fields['sci:doi'] = campaign_dois[0]

            if campaign_citations and len(np.unique(campaign_citations)) == 1:
                if SCI_EXT not in campaign_extensions:
                    campaign_extensions.append(SCI_EXT)
                campaign_extra_fields['sci:citation'] = campaign_citations[0]

            # Collect SAR metadata from all campaign items
            campaign_center_frequencies = [
                item.properties.get('sar:center_frequency')
                for item in all_campaign_items
                if item.properties.get('sar:center_frequency') is not None
            ]
            campaign_bandwidths = [
                item.properties.get('sar:bandwidth')
                for item in all_campaign_items
                if item.properties.get('sar:bandwidth') is not None
            ]

            if (campaign_center_frequencies and
                    len(np.unique(campaign_center_frequencies)) == 1):
                campaign_extensions.append(SAR_EXT)
                campaign_extra_fields['sar:center_frequency'] = (
                    campaign_center_frequencies[0]
                )

            if (campaign_bandwidths and
                    len(np.unique(campaign_bandwidths)) == 1):
                if SAR_EXT not in campaign_extensions:
                    campaign_extensions.append(SAR_EXT)
                campaign_extra_fields['sar:bandwidth'] = (
                    campaign_bandwidths[0]
                )

            campaign_collection = create_collection(
                collection_id=campaign_name,
                description=(
                    f"{campaign['year']} {campaign['aircraft']} flights "
                    f"over {campaign['location']}"
                ),
                extent=campaign_extent,
                stac_extensions=(
                    campaign_extensions if campaign_extensions else None
                ),
                geometry=campaign_geometry
            )

            # Add scientific extra fields to campaign collection
            for key, value in campaign_extra_fields.items():
                campaign_collection.extra_fields[key] = value

            # Add flight collections as children of campaign collection
            for flight_collection in flight_collections:
                campaign_collection.add_child(flight_collection)

            catalog.add_child(campaign_collection)

            print(
                f"Added campaign collection {campaign_name} with "
                f"{len(flight_collections)} flight collections and "
                f"{len(all_campaign_items)} total items"
            )

    output_path.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    print(f"Catalog saved to {output_path}")
    return catalog


def build_flat_catalog(
    data_root: Path,
    output_path: Path,
    catalog_id: str = "OPR",
    data_product: str = "CSARP_standard",
    extra_data_products: list[str] = ['CSARP_layer'],
    base_url: str = "https://data.cresis.ku.edu/data/rds/",
    max_items: int = None,
    campaign_filter: list = None,
    verbose: bool = False
) -> pystac.Catalog:
    """
    Build flattened STAC catalog for parquet export: catalog -> campaigns -> items.
    
    This creates a simplified structure without flight collections, suitable for
    parquet export to STAC servers. Campaign collections have bbox-only extent
    with no geometry fields, but items keep full LineString geometry.

    Parameters
    ----------
    data_root : Path
        Root directory containing campaign data
    output_path : Path
        Directory where catalog will be saved
    catalog_id : str, optional
        Catalog ID, by default "OPR"
    data_product : str, optional
        Primary data product to process, by default "CSARP_standard"
    extra_data_products : List[str], optional
        Additional data products to include, by default ['CSARP_layer']
    base_url : str, optional
        Base URL for asset hrefs, by default "https://data.cresis.ku.edu/data/rds/"
    max_items : int, optional
        Maximum number of items to process, by default None (all items)
    campaign_filter : List[str], optional
        Specific campaigns to process, by default None (all campaigns)
    verbose : bool, optional
        If True, print details for each item being processed, by default False

    Returns
    -------
    pystac.Catalog
        The built flattened STAC catalog
    """
    catalog = create_catalog(catalog_id=catalog_id)
    campaigns = discover_campaigns(data_root)

    # Filter campaigns if specified
    if campaign_filter:
        campaigns = [c for c in campaigns if c['name'] in campaign_filter]

    for campaign in campaigns:
        campaign_path = Path(campaign['path'])
        campaign_name = campaign['name']

        print(f"Processing campaign: {campaign_name}")

        try:
            flight_lines = discover_flight_lines(
                campaign_path, data_product,
                extra_data_products=extra_data_products
            )
        except FileNotFoundError as e:
            print(f"Warning: Skipping {campaign_name}: {e}")
            continue

        if not flight_lines:
            print(f"Warning: No flight lines found for {campaign_name}")
            continue

        # Limit the number of flight lines processed if specified
        if max_items is not None:
            flight_lines = flight_lines[:max_items]

        # Collect ALL items across all flights for this campaign
        all_campaign_items = []

        for flight_data in flight_lines:
            try:
                items = create_items_from_flight_data(
                    flight_data, base_url, campaign_name, data_product, verbose
                )
                all_campaign_items.extend(items)

                flight_id = flight_data['flight_id']
                print(f"  Added {len(items)} items from flight {flight_id}")

                # Break early if we've hit the max items limit
                if (max_items is not None and
                        len(all_campaign_items) >= max_items):
                    break

            except Exception as e:
                flight_id = flight_data['flight_id']
                print(f"Warning: Failed to process flight {flight_id}: {e}")
                continue

        if all_campaign_items:
            # Create campaign collection with bbox-only extent (no geometry)
            campaign_extent = build_collection_extent(all_campaign_items)
            
            # Collect scientific metadata from all campaign items
            campaign_dois = [
                item.properties.get('sci:doi') for item in all_campaign_items
                if item.properties.get('sci:doi') is not None
            ]
            campaign_citations = [
                item.properties.get('sci:citation')
                for item in all_campaign_items
                if item.properties.get('sci:citation') is not None
            ]

            # Check for unique values and prepare extensions (NO projection extension)
            campaign_extensions = []
            campaign_extra_fields = {}

            if campaign_dois and len(np.unique(campaign_dois)) == 1:
                campaign_extensions.append(SCI_EXT)
                campaign_extra_fields['sci:doi'] = campaign_dois[0]

            if campaign_citations and len(np.unique(campaign_citations)) == 1:
                if SCI_EXT not in campaign_extensions:
                    campaign_extensions.append(SCI_EXT)
                campaign_extra_fields['sci:citation'] = campaign_citations[0]

            # Collect SAR metadata from all campaign items
            campaign_center_frequencies = [
                item.properties.get('sar:center_frequency')
                for item in all_campaign_items
                if item.properties.get('sar:center_frequency') is not None
            ]
            campaign_bandwidths = [
                item.properties.get('sar:bandwidth')
                for item in all_campaign_items
                if item.properties.get('sar:bandwidth') is not None
            ]

            if (campaign_center_frequencies and
                    len(np.unique(campaign_center_frequencies)) == 1):
                campaign_extensions.append(SAR_EXT)
                campaign_extra_fields['sar:center_frequency'] = (
                    campaign_center_frequencies[0]
                )

            if (campaign_bandwidths and
                    len(np.unique(campaign_bandwidths)) == 1):
                if SAR_EXT not in campaign_extensions:
                    campaign_extensions.append(SAR_EXT)
                campaign_extra_fields['sar:bandwidth'] = (
                    campaign_bandwidths[0]
                )

            # Create campaign collection with NO geometry field at all
            campaign_collection = create_collection(
                collection_id=campaign_name,
                description=(
                    f"{campaign['year']} {campaign['aircraft']} flights "
                    f"over {campaign['location']}"
                ),
                extent=campaign_extent,
                stac_extensions=(
                    campaign_extensions if campaign_extensions else None
                )
                # Note: no geometry parameter - bbox-only for parquet compatibility
            )

            # Add scientific extra fields to campaign collection
            for key, value in campaign_extra_fields.items():
                campaign_collection.extra_fields[key] = value

            # Add ALL items directly to campaign collection (flattened structure)
            campaign_collection.add_items(all_campaign_items)
            catalog.add_child(campaign_collection)

            print(
                f"Added flattened campaign collection {campaign_name} with "
                f"{len(all_campaign_items)} total items (no flight collections)"
            )

    output_path.mkdir(parents=True, exist_ok=True)
    catalog.normalize_and_save(
        root_href=str(output_path),
        catalog_type=pystac.CatalogType.SELF_CONTAINED
    )

    print(f"Flattened catalog saved to {output_path}")
    return catalog
