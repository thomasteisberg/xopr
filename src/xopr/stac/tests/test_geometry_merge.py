"""Tests for STAC geometry merging functionality."""

import pytest
from unittest.mock import Mock
from datetime import datetime
import pystac
import shapely.geometry
from shapely.geometry import mapping

from xopr.stac.catalog import (
    merge_item_geometries, 
    merge_flight_geometries,
    build_collection_extent_and_geometry,
    create_collection,
    PROJ_EXT
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


class TestMergeFlightGeometries:
    """Test the merge_flight_geometries function."""

    def test_merge_multiple_linestrings(self):
        """Test merging multiple LineString geometries into MultiLineString."""
        flight_geoms = [
            {
                "type": "LineString",
                "coordinates": [[-45.0, -70.0], [-46.0, -71.0], [-47.0, -72.0]]
            },
            {
                "type": "LineString", 
                "coordinates": [[-48.0, -73.0], [-49.0, -74.0], [-50.0, -75.0]]
            },
            {
                "type": "LineString",
                "coordinates": [[-51.0, -76.0], [-52.0, -77.0], [-53.0, -78.0]]
            }
        ]
        
        # Test
        result = merge_flight_geometries(flight_geoms)
        
        # Assertions
        assert result is not None
        assert result["type"] == "MultiLineString"
        assert len(result["coordinates"]) == 3

    def test_merge_single_linestring(self):
        """Test merging single LineString (should return as-is)."""
        flight_geoms = [
            {
                "type": "LineString",
                "coordinates": [[-45.0, -70.0], [-46.0, -71.0], [-47.0, -72.0]]
            }
        ]
        
        # Test
        result = merge_flight_geometries(flight_geoms)
        
        # Assertions
        assert result is not None
        assert result["type"] == "LineString"
        # Convert to tuples for comparison since shapely may return tuples
        expected_coords = [[-45.0, -70.0], [-46.0, -71.0], [-47.0, -72.0]]
        actual_coords = result["coordinates"]
        assert len(actual_coords) == len(expected_coords)
        for i, coord in enumerate(actual_coords):
            assert list(coord) == expected_coords[i]

    def test_merge_empty_list(self):
        """Test merging empty list of flight geometries."""
        result = merge_flight_geometries([])
        assert result is None

    def test_merge_with_none_geometries(self):
        """Test merging with None geometries in the list."""
        flight_geoms = [
            None,
            {
                "type": "LineString",
                "coordinates": [[-45.0, -70.0], [-46.0, -71.0]]
            },
            None
        ]
        
        result = merge_flight_geometries(flight_geoms)
        assert result is not None
        assert result["type"] == "LineString"


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


class TestCreateCollectionWithGeometry:
    """Test the create_collection function with geometry parameter."""

    def test_collection_with_geometry_adds_projection_extension(self):
        """Test that providing geometry automatically adds projection extension."""
        # Create a mock extent
        extent = Mock(spec=pystac.Extent)
        
        # Create geometry
        geometry = {
            "type": "LineString",
            "coordinates": [[-45.0, -70.0], [-46.0, -71.0]]
        }
        
        # Test
        collection = create_collection(
            collection_id="test_collection",
            description="Test collection",
            extent=extent,
            geometry=geometry
        )
        
        # Assertions
        assert PROJ_EXT in collection.stac_extensions
        assert collection.extra_fields.get("proj:geometry") == geometry

    def test_collection_without_geometry_no_projection_extension(self):
        """Test that not providing geometry doesn't add projection extension."""
        # Create a mock extent
        extent = Mock(spec=pystac.Extent)
        
        # Test
        collection = create_collection(
            collection_id="test_collection",
            description="Test collection", 
            extent=extent
        )
        
        # Assertions
        assert PROJ_EXT not in collection.stac_extensions
        assert "proj:geometry" not in collection.extra_fields

    def test_collection_preserves_existing_extensions(self):
        """Test that existing extensions are preserved when adding projection."""
        # Create a mock extent
        extent = Mock(spec=pystac.Extent)
        
        # Create geometry
        geometry = {
            "type": "LineString",
            "coordinates": [[-45.0, -70.0], [-46.0, -71.0]]
        }
        
        existing_extensions = ["https://example.com/extension"]
        
        # Test
        collection = create_collection(
            collection_id="test_collection",
            description="Test collection",
            extent=extent,
            stac_extensions=existing_extensions,
            geometry=geometry
        )
        
        # Assertions
        assert "https://example.com/extension" in collection.stac_extensions
        assert PROJ_EXT in collection.stac_extensions
        assert len(collection.stac_extensions) == 2