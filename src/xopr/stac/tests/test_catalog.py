"""Tests for STAC catalog creation functionality."""

import numpy as np
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

import pystac

from xopr.stac.catalog import create_items_from_flight_data, build_collection_extent
from .common import (create_mock_metadata, create_mock_flight_data, TEST_DOI, 
                     TEST_CITATION, SCI_EXT, SAR_EXT, get_test_config)


class TestCreateItemsFromFlightData:
    """Test the create_items_from_flight_data function."""

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_items_without_scientific_metadata(self, mock_extract):
        """Test that SCI_EXT is not added when doi and citation are None."""
        # Setup
        mock_extract.return_value = create_mock_metadata(doi=None, citation=None)
        flight_data = create_mock_flight_data()
        
        # Test
        items = create_items_from_flight_data(flight_data, get_test_config())
        
        # Assertions
        assert len(items) == 2  # Two data files in mock flight data
        item = items[0]
        
        # Should not have scientific properties
        assert 'sci:doi' not in item.properties
        assert 'sci:citation' not in item.properties
        
        # Should not have scientific extension
        assert SCI_EXT not in item.stac_extensions
        
        # Should have SAR properties and extension
        assert 'sar:center_frequency' in item.properties
        assert 'sar:bandwidth' in item.properties
        assert SAR_EXT in item.stac_extensions

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_items_with_doi_only(self, mock_extract):
        """Test that SCI_EXT is added when doi exists but citation is None."""
        # Setup
        mock_extract.return_value = create_mock_metadata(doi=TEST_DOI, citation=None)
        flight_data = create_mock_flight_data()
        
        # Test
        items = create_items_from_flight_data(flight_data, get_test_config())
        
        # Assertions
        assert len(items) == 2
        item = items[0]
        
        # Should have doi property but not citation
        assert item.properties['sci:doi'] == TEST_DOI
        assert 'sci:citation' not in item.properties
        
        # Should have scientific extension
        assert SCI_EXT in item.stac_extensions

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_items_with_citation_only(self, mock_extract):
        """Test that SCI_EXT is added when citation exists but doi is None."""
        # Setup
        mock_extract.return_value = create_mock_metadata(doi=None, citation=TEST_CITATION)
        flight_data = create_mock_flight_data()
        
        # Test
        items = create_items_from_flight_data(flight_data, get_test_config())
        
        # Assertions
        assert len(items) == 2
        item = items[0]
        
        # Should have citation property but not doi
        assert item.properties['sci:citation'] == TEST_CITATION
        assert 'sci:doi' not in item.properties
        
        # Should have scientific extension
        assert SCI_EXT in item.stac_extensions

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_items_with_both_doi_and_citation(self, mock_extract):
        """Test that SCI_EXT is added when both doi and citation exist."""
        # Setup
        mock_extract.return_value = create_mock_metadata(doi=TEST_DOI, citation=TEST_CITATION)
        flight_data = create_mock_flight_data()
        
        # Test
        items = create_items_from_flight_data(flight_data, get_test_config())
        
        # Assertions
        assert len(items) == 2
        item = items[0]
        
        # Should have both properties
        assert item.properties['sci:doi'] == TEST_DOI
        assert item.properties['sci:citation'] == TEST_CITATION
        
        # Should have scientific extension
        assert SCI_EXT in item.stac_extensions

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_metadata_extraction_failure(self, mock_extract):
        """Test that items with failed metadata extraction are skipped."""
        # Setup - make extract_item_metadata raise an exception
        mock_extract.side_effect = Exception("Metadata extraction failed")
        flight_data = create_mock_flight_data()
        
        # Test
        with patch('builtins.print'):  # Suppress warning print
            items = create_items_from_flight_data(flight_data, get_test_config())
        
        # Assertions
        assert len(items) == 0  # No items should be created

    @patch('xopr.stac.catalog.extract_item_metadata')
    def test_sar_properties_as_python_types(self, mock_extract):
        """Test that SAR properties are stored as Python float types."""
        # Setup
        mock_extract.return_value = create_mock_metadata()
        flight_data = create_mock_flight_data()
        
        # Test
        items = create_items_from_flight_data(flight_data, get_test_config())
        
        # Assertions
        assert len(items) == 2
        item = items[0]
        
        # Check types are Python float, not numpy
        assert isinstance(item.properties['sar:center_frequency'], float)
        assert isinstance(item.properties['sar:bandwidth'], float)
        assert not isinstance(item.properties['sar:center_frequency'], np.floating)
        assert not isinstance(item.properties['sar:bandwidth'], np.floating)


class TestBuildCollectionExtent:
    """Test the build_collection_extent function."""

    def create_mock_item(self, bbox, datetime_obj):
        """Create a mock STAC item for testing."""
        item = Mock(spec=pystac.Item)
        item.bbox = bbox
        item.datetime = datetime_obj
        return item

    def test_empty_items_list_raises_error(self):
        """Test that ValueError is raised for empty items list."""
        with pytest.raises(ValueError, match="Cannot build extent from empty item list"):
            build_collection_extent([])

    def test_single_item_extent(self):
        """Test extent calculation with a single item."""
        # Setup
        bbox = [-69.86, -71.37, -69.84, -71.35]
        dt = datetime(2016, 10, 14, 16, 12, 44)
        item = self.create_mock_item(bbox, dt)
        
        # Test
        extent = build_collection_extent([item])
        
        # Assertions
        assert len(extent.spatial.bboxes) == 1
        assert extent.spatial.bboxes[0] == bbox
        
        assert len(extent.temporal.intervals) == 1
        assert extent.temporal.intervals[0] == [dt, dt]

    def test_multiple_items_extent(self):
        """Test extent calculation with multiple items."""
        # Setup
        bbox1 = [-69.86, -71.37, -69.84, -71.35]
        bbox2 = [-69.90, -71.40, -69.80, -71.30]
        dt1 = datetime(2016, 10, 14, 16, 12, 44)
        dt2 = datetime(2016, 10, 14, 17, 15, 30)
        
        item1 = self.create_mock_item(bbox1, dt1)
        item2 = self.create_mock_item(bbox2, dt2)
        
        # Test
        extent = build_collection_extent([item1, item2])
        
        # Assertions
        # Spatial extent should be union of bboxes
        assert len(extent.spatial.bboxes) == 1
        union_bbox = extent.spatial.bboxes[0]
        assert union_bbox[0] == min(bbox1[0], bbox2[0])  # min x
        assert union_bbox[1] == min(bbox1[1], bbox2[1])  # min y
        assert union_bbox[2] == max(bbox1[2], bbox2[2])  # max x
        assert union_bbox[3] == max(bbox1[3], bbox2[3])  # max y
        
        # Temporal extent should span from earliest to latest
        assert len(extent.temporal.intervals) == 1
        assert extent.temporal.intervals[0] == [dt1, dt2]  # dt1 is earlier

    def test_items_with_none_values(self):
        """Test extent calculation when some items have None bbox or datetime."""
        # Setup
        bbox = [-69.86, -71.37, -69.84, -71.35]
        dt = datetime(2016, 10, 14, 16, 12, 44)
        
        item_with_data = self.create_mock_item(bbox, dt)
        item_without_bbox = self.create_mock_item(None, dt)
        item_without_datetime = self.create_mock_item(bbox, None)
        
        # Test
        extent = build_collection_extent([item_with_data, item_without_bbox, item_without_datetime])
        
        # Assertions
        # Should use data from valid items
        assert len(extent.spatial.bboxes) == 1
        assert extent.spatial.bboxes[0] == bbox
        
        assert len(extent.temporal.intervals) == 1
        assert extent.temporal.intervals[0] == [dt, dt]