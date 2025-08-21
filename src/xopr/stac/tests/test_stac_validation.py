"""Tests for STAC validation using stac-validator for our library functions."""

import json
import tempfile
from pathlib import Path
import pytest
from unittest.mock import Mock, patch

from stac_validator.validate import StacValidate

from xopr.stac.catalog import (
    create_catalog, create_collection, create_item, create_items_from_flight_data,
    build_limited_catalog
)
from .common import (
    create_mock_metadata, create_mock_flight_data, create_mock_campaign_data,
    TEST_DOI, TEST_CITATION
)


class TestSTACValidation:
    """Test STAC validation for objects created by our library functions."""
    
    def _validate_stac_via_file(self, stac_object):
        """Helper method to validate STAC objects via temporary file.
        
        Parameters
        ----------
        stac_object : pystac object
            STAC object (catalog, collection, or item) to validate
            
        Returns
        -------
        bool
            True if valid, False if invalid
        """
        # Write to temporary file and validate via file path
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(stac_object.to_dict(), f, indent=2, default=str)
            temp_file = f.name
        
        try:
            validator = StacValidate(stac_file=temp_file)
            result = validator.run()
            return result
        finally:
            Path(temp_file).unlink()

    def test_validate_create_catalog(self):
        """Test that catalogs created by create_catalog() are valid."""
        catalog = create_catalog(
            catalog_id="test-catalog",
            description="Test catalog for validation"
        )
        
        catalog_dict = catalog.to_dict()
        validator = StacValidate()
        result = validator.validate_dict(catalog_dict)
        assert result is True, f"create_catalog() produced invalid catalog"

    def test_validate_create_collection(self):
        """Test that collections created by create_collection() are valid."""
        import pystac
        from datetime import datetime
        
        # Create extent as required by create_collection
        spatial_extent = pystac.SpatialExtent(bboxes=[[-69.86, -71.37, -69.84, -71.35]])
        temporal_extent = pystac.TemporalExtent(intervals=[[datetime(2016, 10, 14), datetime(2016, 10, 14)]])
        extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)
        
        collection = create_collection(
            collection_id="test-collection",
            description="Test collection for validation",
            extent=extent,
            license="various"
        )
        
        collection_dict = collection.to_dict()
        validator = StacValidate()
        result = validator.validate_dict(collection_dict)
        assert result is True, f"create_collection() produced invalid collection"

    def test_validate_create_item(self):
        """Test that items created by create_item() are valid."""
        from shapely.geometry import Point, mapping
        from datetime import datetime
        
        # Create geometry and bbox for create_item
        point = Point(-69.85, -71.36)
        geometry = mapping(point)
        bbox = [-69.86, -71.37, -69.84, -71.35]
        
        item = create_item(
            item_id="test-item",
            geometry=geometry,
            bbox=bbox,
            datetime=datetime(2016, 10, 14, 16, 12, 44),
            properties={"test": "value"}
        )
        
        result = self._validate_stac_via_file(item)
        assert result is True, f"create_item() produced invalid item"

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_validate_create_items_from_flight_data(self, mock_extract):
        """Test that items created by create_items_from_flight_data() are valid."""
        # Setup mock with both scientific and SAR metadata
        mock_extract.return_value = create_mock_metadata(
            doi=TEST_DOI, 
            citation=TEST_CITATION,
            frequency=190e6,
            bandwidth=50e6
        )
        flight_data = create_mock_flight_data()
        
        # Create items using our library function
        items = create_items_from_flight_data(
            flight_data,
            base_url="https://test.example.com/",
            campaign_name="test_campaign"
        )
        
        # Validate each item created by our function
        assert len(items) > 0, "create_items_from_flight_data() should create items"
        
        for i, item in enumerate(items):
            result = self._validate_stac_via_file(item)
            assert result is True, f"create_items_from_flight_data() produced invalid item {i}"

    @patch('xopr.stac.catalog.discover_campaigns')
    @patch('xopr.stac.catalog.discover_flight_lines')
    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_validate_build_limited_catalog(self, mock_extract, mock_flight_lines, mock_campaigns):
        """Test that catalogs created by build_limited_catalog() are valid."""
        from pathlib import Path
        import tempfile
        
        # Setup mocks for a complete catalog build
        mock_campaigns.return_value = [create_mock_campaign_data()]
        mock_flight_lines.return_value = [create_mock_flight_data()]
        mock_extract.return_value = create_mock_metadata(
            doi=TEST_DOI,
            citation=TEST_CITATION,
            frequency=190e6,
            bandwidth=50e6
        )
        
        # Create temporary directories for the test
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "data"
            output_path = Path(temp_dir) / "output"
            data_root.mkdir()
            
            # Build catalog using our library function
            catalog = build_limited_catalog(
                data_root=data_root,
                output_path=output_path,
                catalog_id="test-limited-catalog",
                max_items=2,
                verbose=False
            )
            
            # Validate the root catalog
            catalog_dict = catalog.to_dict()
            validator = StacValidate()
            result = validator.validate_dict(catalog_dict)
            assert result is True, f"build_limited_catalog() produced invalid catalog"
            
            # Validate campaign collections created by the function
            campaign_collections = list(catalog.get_collections())
            assert len(campaign_collections) > 0, "build_limited_catalog() should create campaign collections"
            
            for campaign_collection in campaign_collections:
                collection_dict = campaign_collection.to_dict()
                validator = StacValidate()
                result = validator.validate_dict(collection_dict)
                assert result is True, f"build_limited_catalog() produced invalid campaign collection"
                
                # Validate flight collections created by the function
                flight_collections = list(campaign_collection.get_collections())
                for flight_collection in flight_collections:
                    flight_dict = flight_collection.to_dict()
                    validator = StacValidate()
                    result = validator.validate_dict(flight_dict)
                    assert result is True, f"build_limited_catalog() produced invalid flight collection"
                    
                    # Validate items created by the function
                    items = list(flight_collection.get_items())
                    for item in items:
                        result = self._validate_stac_via_file(item)
                        assert result is True, f"build_limited_catalog() produced invalid item"

    def test_validate_create_collection_with_geometry(self):
        """Test that collections with projection extension created by create_collection() are valid."""
        import pystac
        from shapely.geometry import LineString, mapping
        from datetime import datetime
        
        # Create geometry for projection extension
        line = LineString([(-69.86, -71.35), (-69.85, -71.36), (-69.84, -71.37)])
        geometry = mapping(line)
        
        # Create extent
        spatial_extent = pystac.SpatialExtent(bboxes=[[-69.86, -71.37, -69.84, -71.35]])
        temporal_extent = pystac.TemporalExtent(intervals=[[datetime(2016, 10, 14), datetime(2016, 10, 14)]])
        extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)
        
        # Test create_collection with geometry (which adds projection extension)
        collection = create_collection(
            collection_id="test-projection-collection",
            description="Test collection with projection extension",
            extent=extent,
            geometry=geometry
        )
        
        collection_dict = collection.to_dict()
        validator = StacValidate()
        result = validator.validate_dict(collection_dict)
        assert result is True, f"create_collection() with geometry produced invalid collection"

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_validate_items_with_extensions(self, mock_extract):
        """Test that items with extensions created by create_items_from_flight_data() are valid."""
        # Test with no extensions (minimal case)
        mock_extract.return_value = create_mock_metadata(
            doi=None, citation=None, frequency=None, bandwidth=None
        )
        flight_data = create_mock_flight_data()
        
        items = create_items_from_flight_data(flight_data)
        for item in items:
            result = self._validate_stac_via_file(item)
            assert result is True, f"create_items_from_flight_data() with no extensions produced invalid item"
        
        # Test with scientific extension only
        mock_extract.return_value = create_mock_metadata(
            doi=TEST_DOI, citation=None, frequency=None, bandwidth=None
        )
        
        items = create_items_from_flight_data(flight_data)
        for item in items:
            result = self._validate_stac_via_file(item)
            assert result is True, f"create_items_from_flight_data() with scientific extension produced invalid item"
        
        # Test with SAR extension only
        mock_extract.return_value = create_mock_metadata(
            doi=None, citation=None, frequency=190e6, bandwidth=50e6
        )
        
        items = create_items_from_flight_data(flight_data)
        for item in items:
            result = self._validate_stac_via_file(item)
            assert result is True, f"create_items_from_flight_data() with SAR extension produced invalid item"
        
        # Test with both extensions
        mock_extract.return_value = create_mock_metadata(
            doi=TEST_DOI, citation=TEST_CITATION, frequency=190e6, bandwidth=50e6
        )
        
        items = create_items_from_flight_data(flight_data)
        for item in items:
            result = self._validate_stac_via_file(item)
            assert result is True, f"create_items_from_flight_data() with both extensions produced invalid item"

    def test_invalid_stac_objects_fail_validation(self):
        """Test that invalid STAC objects are correctly rejected by the validator."""
        validator = StacValidate()
        
        # Test invalid catalog (missing required fields)
        invalid_catalog = {
            "type": "Catalog",
            "stac_version": "1.1.0",
            # Missing required 'id' and 'description' fields
        }
        result = validator.validate_dict(invalid_catalog)
        assert result is False, "Invalid catalog should fail validation"
        
        # Test invalid collection (missing required fields)
        invalid_collection = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "test-collection",
            # Missing required 'description', 'license', and 'extent' fields
        }
        result = validator.validate_dict(invalid_collection)
        assert result is False, "Invalid collection should fail validation"
        
        # Test invalid item (missing required fields)
        invalid_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "test-item",
            # Missing required 'geometry', 'bbox', 'properties', and 'datetime'
        }
        result = validator.validate_dict(invalid_item)
        assert result is False, "Invalid item should fail validation"
        
        # Test item with invalid geometry
        invalid_geometry_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "test-item",
            "geometry": "not-a-geometry",  # Invalid geometry
            "bbox": [-69.86, -71.37, -69.84, -71.35],
            "properties": {"datetime": "2016-10-14T16:12:44Z"},
            "links": [],
            "assets": {}
        }
        result = validator.validate_dict(invalid_geometry_item)
        assert result is False, "Item with invalid geometry should fail validation"
        
        # Test item with invalid bbox (wrong number of coordinates)
        invalid_bbox_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "test-item",
            "geometry": {"type": "Point", "coordinates": [-69.85, -71.36]},
            "bbox": [-69.86, -71.37],  # Invalid bbox (should have 4 coordinates)
            "properties": {"datetime": "2016-10-14T16:12:44Z"},
            "links": [],
            "assets": {}
        }
        result = validator.validate_dict(invalid_bbox_item)
        assert result is False, "Item with invalid bbox should fail validation"

