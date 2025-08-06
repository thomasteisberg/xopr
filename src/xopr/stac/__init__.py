"""
STAC catalog creation utilities for Open Polar Radar data.

This module provides tools for generating STAC (SpatioTemporal Asset Catalog)
metadata for OPR datasets, enabling spatial and temporal search capabilities
across radar campaigns and data products.
"""

from .catalog import (
    create_catalog, create_collection, create_item,
    build_collection_extent, create_items_from_flight_data
)
from .metadata import extract_item_metadata, discover_campaigns, discover_flight_lines

__all__ = [
    "create_catalog",
    "create_collection", 
    "create_item",
    "build_collection_extent",
    "create_items_from_flight_data",
    "extract_item_metadata",
    "discover_campaigns",
    "discover_flight_lines"
]