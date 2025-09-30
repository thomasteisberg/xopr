"""Tests for STAC geometry merging functionality."""

import pytest
from unittest.mock import Mock
from datetime import datetime
import pystac
import shapely.geometry
from shapely.geometry import mapping

from xopr.stac.catalog import create_collection
from xopr.stac.geometry import (
    merge_item_geometries,
    merge_flight_geometries,
    build_collection_extent_and_geometry
)


class TestMergeItemGeometries:
    """Test the merge_item_geometries function."""

    def test_merge_antarctic_linestrings(self):
        """Test merging LineString geometries in Antarctic region."""
        # Create mock STAC items with Antarctic coordinates
        item1 = Mock(spec=pystac.Item)
        item1.geometry = {
            "type": "LineString",
            "coordinates": [[-45.0, -70.0], [-46.0, -71.0], [-47.0, -72.0]]
        }
        item1.properties = {}  # Add properties attribute
        
        item2 = Mock(spec=pystac.Item)
        item2.geometry = {
            "type": "LineString", 
            "coordinates": [[-47.0, -72.0], [-48.0, -73.0], [-49.0, -74.0]]
        }
        item2.properties = {}  # Add properties attribute
        
        items = [item1, item2]
        
        # Test
        result = merge_item_geometries(items)
        
        # Assertions
        assert result is not None
        assert result["type"] in ["LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        assert "coordinates" in result

    def test_merge_arctic_linestrings(self):
        """Test merging LineString geometries in Arctic region."""
        # Create mock STAC items with Arctic coordinates
        item1 = Mock(spec=pystac.Item)
        item1.geometry = {
            "type": "LineString",
            "coordinates": [[-45.0, 70.0], [-46.0, 71.0], [-47.0, 72.0]]
        }
        item1.properties = {}  # Add properties attribute
        
        item2 = Mock(spec=pystac.Item)
        item2.geometry = {
            "type": "LineString", 
            "coordinates": [[-47.0, 72.0], [-48.0, 73.0], [-49.0, 74.0]]
        }
        item2.properties = {}  # Add properties attribute
        
        items = [item1, item2]
        
        # Test
        result = merge_item_geometries(items)
        
        # Assertions
        assert result is not None
        assert result["type"] in ["LineString", "MultiLineString", "Polygon", "MultiPolygon"]
        assert "coordinates" in result

    def test_merge_empty_list(self):
        """Test merging empty list of items."""
        result = merge_item_geometries([])
        assert result is None

    def test_merge_items_without_geometry(self):
        """Test merging items that have no geometry."""
        item1 = Mock(spec=pystac.Item)
        item1.geometry = None
        item1.properties = {}  # Add properties attribute
        
        item2 = Mock(spec=pystac.Item)
        item2.geometry = None
        item2.properties = {}  # Add properties attribute
        
        result = merge_item_geometries([item1, item2])
        assert result is None

    def test_custom_simplify_tolerance(self):
        """Test that custom simplify tolerance parameter works."""
        item1 = Mock(spec=pystac.Item)
        item1.geometry = {
            "type": "LineString",
            "coordinates": [[-45.0, -70.0], [-46.0, -71.0], [-47.0, -72.0]]
        }
        item1.properties = {}  # Add properties attribute
        
        # Test with different tolerance
        result = merge_item_geometries([item1], simplify_tolerance=50.0)
        assert result is not None


class TestBuildCollectionExtentAndGeometry:
    """Test the build_collection_extent_and_geometry function."""

    def test_extent_and_geometry_creation(self):
        """Test that both extent and geometry are created properly."""
        # Create mock STAC items
        item1 = Mock(spec=pystac.Item)
        item1.geometry = {
            "type": "LineString",
            "coordinates": [[-45.0, -70.0], [-46.0, -71.0]]
        }
        item1.bbox = [-46.0, -71.0, -45.0, -70.0]
        item1.datetime = datetime(2023, 1, 1, 12, 0, 0)
        item1.properties = {}  # Add properties attribute
        
        item2 = Mock(spec=pystac.Item)
        item2.geometry = {
            "type": "LineString",
            "coordinates": [[-47.0, -72.0], [-48.0, -73.0]]
        }
        item2.bbox = [-48.0, -73.0, -47.0, -72.0]
        item2.datetime = datetime(2023, 1, 2, 12, 0, 0)
        item2.properties = {}  # Add properties attribute
        
        items = [item1, item2]
        
        # Test
        extent, geometry = build_collection_extent_and_geometry(items)
        
        # Assertions
        assert isinstance(extent, pystac.Extent)
        assert extent.spatial is not None
        assert extent.temporal is not None
        assert geometry is not None
        assert "type" in geometry
        assert "coordinates" in geometry

    def test_empty_items_list(self):
        """Test that empty items list raises ValueError."""
        with pytest.raises(ValueError, match="Cannot build extent from empty item list"):
            build_collection_extent_and_geometry([])
