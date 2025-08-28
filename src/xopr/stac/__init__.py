"""
STAC catalog creation utilities for Open Polar Radar data.

This module provides tools for generating STAC (SpatioTemporal Asset Catalog)
metadata for OPR datasets, enabling spatial and temporal search capabilities
across radar campaigns and data products.
"""

from .catalog import (
    create_catalog, create_collection, create_item,
    create_items_from_flight_data,
    build_limited_catalog, build_flat_collection,
    build_flat_catalog_dask, build_catalog_from_parquet_files,
    export_collection_to_parquet
)
from .config import CatalogConfig
from .geometry import (
    build_collection_extent, build_collection_extent_and_geometry,
    merge_item_geometries, merge_flight_geometries
)
from .metadata import extract_item_metadata, discover_campaigns, discover_flight_lines, collect_uniform_metadata
from .build import (
    process_single_flight, process_single_campaign,
    collect_metadata_from_items, build_hierarchical_catalog,
    export_to_geoparquet,
    export_collections_to_parquet, export_collections_metadata, 
    build_catalog_from_parquet_metadata,
    save_catalog
)

__all__ = [
    # Configuration
    "CatalogConfig",
    # Catalog functions
    "create_catalog",
    "create_collection", 
    "create_item",
    "build_collection_extent",
    "create_items_from_flight_data",
    "build_limited_catalog",
    "build_flat_collection",
    "build_flat_catalog_dask",
    "build_catalog_from_parquet_files",
    # Metadata functions
    "extract_item_metadata",
    "discover_campaigns",
    "discover_flight_lines",
    "collect_uniform_metadata",
    # Build functions
    "process_single_flight",
    "process_single_campaign",
    "collect_metadata_from_items",
    "build_hierarchical_catalog",
    "export_to_geoparquet",
    "export_collection_to_parquet",
    "export_collections_to_parquet",
    "export_collections_metadata",
    "build_catalog_from_parquet_metadata",
    "save_catalog"
]