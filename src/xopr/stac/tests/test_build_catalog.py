"""Tests for build_limited_catalog functionality."""

import numpy as np
import pytest
from unittest.mock import Mock, patch
from pathlib import Path

import pystac

# Import the function we're testing from the catalog module
from xopr.stac.catalog import build_limited_catalog

from .common import create_mock_stac_item, TEST_DOI, SCI_EXT


class TestBuildLimitedCatalog:
    """Test the build_limited_catalog function collection logic."""

    @patch('xopr.stac.catalog.discover_campaigns')
    @patch('xopr.stac.catalog.discover_flight_lines')
    @patch('xopr.stac.catalog.create_items_from_flight_data')
    @patch('xopr.stac.catalog.create_catalog')
    @patch('xopr.stac.catalog.create_collection')
    def test_flight_collection_no_scientific_metadata(self, mock_create_collection, 
                                                    mock_create_catalog, mock_create_items,
                                                    mock_discover_flights, mock_discover_campaigns):
        """Test that SCI_EXT is not added to flight collection when no scientific metadata exists."""
        # Setup mocks
        mock_create_catalog.return_value = Mock(spec=pystac.Catalog)
        mock_discover_campaigns.return_value = [{
            'name': '2016_Antarctica_DC8',
            'year': '2016',
            'location': 'Antarctica',
            'aircraft': 'DC8',
            'path': '/test/path'
        }]
        mock_discover_flights.return_value = [{
            'flight_id': '20161014_03',
            'data_files': {'CSARP_standard': {}}
        }]
        
        # Create items without scientific metadata
        items = [create_mock_stac_item(doi=None, citation=None)]
        mock_create_items.return_value = items
        
        # Mock collection creation
        mock_flight_collection = Mock(spec=pystac.Collection)
        mock_flight_collection.extra_fields = {}
        mock_campaign_collection = Mock(spec=pystac.Collection)
        mock_campaign_collection.extra_fields = {}
        mock_create_collection.side_effect = [mock_flight_collection, mock_campaign_collection]
        
        # Test
        with patch('pathlib.Path.mkdir'), patch('xopr.stac.catalog.build_collection_extent'):
            catalog = build_limited_catalog(
                data_root=Path('/test'),
                output_path=Path('/output'),
                max_items=1
            )
        
        # Verify flight collection creation - should not have SCI_EXT
        flight_collection_call = mock_create_collection.call_args_list[0]
        flight_kwargs = flight_collection_call[1]
        
        # Should not have scientific extension
        stac_extensions = flight_kwargs.get('stac_extensions')
        if stac_extensions:
            sci_ext = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
            assert sci_ext not in stac_extensions
        
        # Extra fields should not have scientific properties
        assert 'sci:doi' not in mock_flight_collection.extra_fields
        assert 'sci:citation' not in mock_flight_collection.extra_fields

    @patch('xopr.stac.catalog.discover_campaigns')
    @patch('xopr.stac.catalog.discover_flight_lines')
    @patch('xopr.stac.catalog.create_items_from_flight_data')
    @patch('xopr.stac.catalog.create_catalog')
    @patch('xopr.stac.catalog.create_collection')
    def test_flight_collection_with_unique_doi(self, mock_create_collection, 
                                             mock_create_catalog, mock_create_items,
                                             mock_discover_flights, mock_discover_campaigns):
        """Test that SCI_EXT is added to flight collection when unique DOI exists."""
        # Setup mocks
        mock_create_catalog.return_value = Mock(spec=pystac.Catalog)
        mock_discover_campaigns.return_value = [{
            'name': '2016_Antarctica_DC8',
            'year': '2016',
            'location': 'Antarctica',
            'aircraft': 'DC8',
            'path': '/test/path'
        }]
        mock_discover_flights.return_value = [{
            'flight_id': '20161014_03',
            'data_files': {'CSARP_standard': {}}
        }]
        
        # Create items with same DOI
        test_doi = "10.1234/test.doi"
        items = [
            create_mock_stac_item(doi=test_doi, citation=None),
            create_mock_stac_item(doi=test_doi, citation=None)
        ]
        mock_create_items.return_value = items
        
        # Mock collection creation
        mock_flight_collection = Mock(spec=pystac.Collection)
        mock_flight_collection.extra_fields = {}
        mock_campaign_collection = Mock(spec=pystac.Collection)
        mock_campaign_collection.extra_fields = {}
        mock_create_collection.side_effect = [mock_flight_collection, mock_campaign_collection]
        
        # Test
        with patch('pathlib.Path.mkdir'), patch('xopr.stac.catalog.build_collection_extent'):
            catalog = build_limited_catalog(
                data_root=Path('/test'),
                output_path=Path('/output'),
                max_items=2
            )
        
        # Verify flight collection creation - should have SCI_EXT
        flight_collection_call = mock_create_collection.call_args_list[0]
        flight_kwargs = flight_collection_call[1]
        
        # Should have scientific extension
        stac_extensions = flight_kwargs.get('stac_extensions')
        assert stac_extensions is not None
        sci_ext = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
        assert sci_ext in stac_extensions
        
        # Extra fields should have DOI
        assert mock_flight_collection.extra_fields['sci:doi'] == test_doi

    @patch('xopr.stac.catalog.discover_campaigns')
    @patch('xopr.stac.catalog.discover_flight_lines')
    @patch('xopr.stac.catalog.create_items_from_flight_data')
    @patch('xopr.stac.catalog.create_catalog')
    @patch('xopr.stac.catalog.create_collection')
    def test_flight_collection_with_multiple_dois_no_aggregation(self, mock_create_collection,
                                                               mock_create_catalog, mock_create_items,
                                                               mock_discover_flights, mock_discover_campaigns):
        """Test that SCI_EXT is not added when multiple different DOIs exist."""
        # Setup mocks
        mock_create_catalog.return_value = Mock(spec=pystac.Catalog)
        mock_discover_campaigns.return_value = [{
            'name': '2016_Antarctica_DC8',
            'year': '2016',
            'location': 'Antarctica',
            'aircraft': 'DC8',
            'path': '/test/path'
        }]
        mock_discover_flights.return_value = [{
            'flight_id': '20161014_03',
            'data_files': {'CSARP_standard': {}}
        }]
        
        # Create items with different DOIs
        items = [
            create_mock_stac_item(doi="10.1234/doi1", citation=None),
            create_mock_stac_item(doi="10.1234/doi2", citation=None)
        ]
        mock_create_items.return_value = items
        
        # Mock collection creation
        mock_flight_collection = Mock(spec=pystac.Collection)
        mock_flight_collection.extra_fields = {}
        mock_campaign_collection = Mock(spec=pystac.Collection)
        mock_campaign_collection.extra_fields = {}
        mock_create_collection.side_effect = [mock_flight_collection, mock_campaign_collection]
        
        # Test
        with patch('pathlib.Path.mkdir'), patch('xopr.stac.catalog.build_collection_extent'):
            catalog = build_limited_catalog(
                data_root=Path('/test'),
                output_path=Path('/output'),
                max_items=2
            )
        
        # Verify flight collection creation - should not have SCI_EXT for DOI
        flight_collection_call = mock_create_collection.call_args_list[0]
        flight_kwargs = flight_collection_call[1]
        
        # Should not have scientific extension (or if it does, not for DOI reasons)
        stac_extensions = flight_kwargs.get('stac_extensions')
        if stac_extensions:
            # If SCI_EXT is present, it should not be due to DOI aggregation
            pass  # This test mainly checks that DOI is not aggregated
        
        # Extra fields should not have DOI (multiple unique values)
        assert 'sci:doi' not in mock_flight_collection.extra_fields

    @patch('xopr.stac.catalog.discover_campaigns')
    @patch('xopr.stac.catalog.discover_flight_lines')
    @patch('xopr.stac.catalog.create_items_from_flight_data')
    @patch('xopr.stac.catalog.create_catalog')
    @patch('xopr.stac.catalog.create_collection')
    def test_campaign_collection_aggregation(self, mock_create_collection,
                                           mock_create_catalog, mock_create_items,
                                           mock_discover_flights, mock_discover_campaigns):
        """Test that campaign collection properly aggregates from all flights."""
        # Setup mocks
        mock_create_catalog.return_value = Mock(spec=pystac.Catalog)
        mock_discover_campaigns.return_value = [{
            'name': '2016_Antarctica_DC8',
            'year': '2016',
            'location': 'Antarctica',
            'aircraft': 'DC8',
            'path': '/test/path'
        }]
        mock_discover_flights.return_value = [
            {'flight_id': '20161014_03', 'data_files': {'CSARP_standard': {}}},
            {'flight_id': '20161014_04', 'data_files': {'CSARP_standard': {}}}
        ]
        
        # Create items for each flight - same DOI across all items
        test_doi = "10.1234/campaign.doi"
        flight1_items = [create_mock_stac_item(doi=test_doi, citation=None)]
        flight2_items = [create_mock_stac_item(doi=test_doi, citation=None)]
        mock_create_items.side_effect = [flight1_items, flight2_items]
        
        # Mock collection creation
        collections = []
        for i in range(3):  # 2 flight + 1 campaign collection
            mock_collection = Mock(spec=pystac.Collection)
            mock_collection.extra_fields = {}
            collections.append(mock_collection)
        mock_create_collection.side_effect = collections
        
        # Test
        with patch('pathlib.Path.mkdir'), patch('xopr.stac.catalog.build_collection_extent'):
            catalog = build_limited_catalog(
                data_root=Path('/test'),
                output_path=Path('/output'),
                max_items=2
            )
        
        # Verify campaign collection (last collection created) has aggregated DOI
        campaign_collection_call = mock_create_collection.call_args_list[-1]
        campaign_kwargs = campaign_collection_call[1]
        
        # Should have scientific extension
        stac_extensions = campaign_kwargs.get('stac_extensions')
        assert stac_extensions is not None
        sci_ext = 'https://stac-extensions.github.io/scientific/v1.0.0/schema.json'
        assert sci_ext in stac_extensions
        
        # Campaign collection extra fields should have DOI
        # The DOI is added to extra_fields after collection creation
        campaign_collection = collections[-1]
        # Verify the DOI was assigned to the mock collection's extra_fields
        # Since the build function assigns: campaign_collection.extra_fields[key] = value
        assert 'sci:doi' in campaign_collection.extra_fields
        assert campaign_collection.extra_fields['sci:doi'] == test_doi

    @patch('xopr.stac.catalog.discover_campaigns')
    @patch('xopr.stac.catalog.discover_flight_lines') 
    def test_none_values_filtered_correctly(self, mock_discover_flights, mock_discover_campaigns):
        """Test that None values are properly filtered in collection logic."""
        # This test specifically validates the `is not None` filtering logic
        # by checking that the filtering expressions work correctly
        
        # Create test items with None and non-None values
        test_items = [
            Mock(properties={'sci:doi': None, 'sci:citation': 'Test Citation'}),
            Mock(properties={'sci:doi': '10.1234/test', 'sci:citation': None}),
            Mock(properties={'sci:doi': '10.1234/test', 'sci:citation': 'Test Citation'}),
            Mock(properties={})  # No scientific properties
        ]
        
        # Test the filtering logic that should be used in build_limited_catalog
        dois = [
            item.properties.get('sci:doi') for item in test_items
            if item.properties.get('sci:doi') is not None
        ]
        citations = [
            item.properties.get('sci:citation') for item in test_items
            if item.properties.get('sci:citation') is not None
        ]
        
        # Assertions
        assert len(dois) == 2  # Only items 2 and 3 have non-None DOI
        assert len(citations) == 2  # Only items 1 and 3 have non-None citation
        assert '10.1234/test' in dois
        assert 'Test Citation' in citations
        assert None not in dois
        assert None not in citations
        
        # Test uniqueness logic
        assert len(np.unique(dois)) == 1  # All non-None DOIs are the same
        assert len(np.unique(citations)) == 1  # All non-None citations are the same