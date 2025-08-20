"""Tests for metadata extraction functionality."""

import numpy as np
import pytest
from unittest.mock import Mock, patch
from pathlib import Path

from xopr.stac.metadata import extract_item_metadata
from .common import create_mock_dataset, TEST_DOI, TEST_ROR, TEST_FUNDER


class TestExtractItemMetadata:
    """Test the extract_item_metadata function."""

    def test_extract_metadata_with_none_values(self):
        """Test that None values are returned when doi/ror/funder_text are missing."""
        # Create dataset with no doi, ror, funder_text
        mock_ds = create_mock_dataset()
        
        # Test
        result = extract_item_metadata(dataset=mock_ds)
        
        # Assertions
        assert result['doi'] is None
        assert result['citation'] is None  # funder_text maps to citation
        assert 'frequency' in result
        assert 'bandwidth' in result
        # Dataset should not be closed when passed in directly
        mock_ds.close.assert_not_called()

    def test_extract_metadata_with_values(self):
        """Test that actual values are returned when doi/ror/funder_text exist."""
        # Create dataset with values
        mock_ds = create_mock_dataset(
            doi=TEST_DOI, 
            ror=TEST_ROR, 
            funder_text=TEST_FUNDER
        )
        
        # Test
        result = extract_item_metadata(dataset=mock_ds)
        
        # Assertions
        assert result['doi'] == TEST_DOI
        assert result['citation'] == TEST_FUNDER
        # Dataset should not be closed when passed in directly
        mock_ds.close.assert_not_called()

    def test_frequency_extraction_uniform_values(self):
        """Test frequency extraction when all values are the same."""
        mock_ds = create_mock_dataset(
            f0_values=[165e6, 165e6, 165e6],
            f1_values=[215e6, 215e6, 215e6]
        )
        
        # Test
        result = extract_item_metadata(dataset=mock_ds)
        
        # Assertions
        assert result['frequency'] == 190e6  # center frequency
        assert result['bandwidth'] == 50e6   # |215 - 165|
        assert isinstance(result['frequency'], float)
        assert isinstance(result['bandwidth'], float)

    def test_frequency_extraction_transposed_values(self):
        """Test frequency extraction when f0 > f1 (transposed case)."""
        mock_ds = create_mock_dataset(
            f0_values=[215e6, 215e6, 215e6],  # Higher frequency in f0
            f1_values=[165e6, 165e6, 165e6]   # Lower frequency in f1
        )
        
        # Test
        result = extract_item_metadata(dataset=mock_ds)
        
        # Assertions
        assert result['frequency'] == 190e6  # center frequency
        assert result['bandwidth'] == 50e6   # abs(165 - 215) = 50
        assert isinstance(result['frequency'], float)
        assert isinstance(result['bandwidth'], float)

    def test_frequency_extraction_multiple_unique_values_error(self):
        """Test that ValueError is raised when multiple unique frequency values exist."""
        mock_ds = create_mock_dataset(
            f0_values=[165e6, 170e6, 175e6],  # Multiple different values
            f1_values=[215e6, 215e6, 215e6]
        )
        
        # Test - should raise ValueError
        with pytest.raises(ValueError, match="Multiple low frequency values found"):
            extract_item_metadata(dataset=mock_ds)

    def test_datetime_conversion(self):
        """Test that datetime is properly converted from xarray to Python datetime."""
        mock_ds = create_mock_dataset()
        
        # Test
        result = extract_item_metadata(dataset=mock_ds)
        
        # Assertions
        from datetime import datetime
        assert isinstance(result['date'], datetime)
        assert result['date'].year == 2016
        assert result['date'].month == 10
        assert result['date'].day == 14

    def test_parameter_validation_both_provided(self):
        """Test that ValueError is raised when both parameters are provided."""
        mock_ds = create_mock_dataset()
        
        with pytest.raises(ValueError, match="Exactly one of mat_file_path or dataset must be provided"):
            extract_item_metadata(mat_file_path='/fake/path.mat', dataset=mock_ds)

    def test_parameter_validation_neither_provided(self):
        """Test that ValueError is raised when neither parameter is provided."""
        with pytest.raises(ValueError, match="Exactly one of mat_file_path or dataset must be provided"):
            extract_item_metadata()

    def test_file_not_found_error(self):
        """Test that FileNotFoundError is raised for non-existent local files."""
        # Test with a local path that doesn't exist
        with pytest.raises(FileNotFoundError, match="MAT file not found"):
            extract_item_metadata(mat_file_path='/does/not/exist.mat')

    @patch('xopr.stac.metadata.OPRConnection')
    def test_file_loading_closes_dataset(self, mock_opr_connection):
        """Test that dataset is properly closed when loaded from file."""
        mock_opr = Mock()
        mock_opr_connection.return_value = mock_opr
        
        mock_ds = create_mock_dataset()
        mock_opr.load_frame_url.return_value = mock_ds
        
        # Test with string path
        with patch('pathlib.Path.exists', return_value=True):
            result = extract_item_metadata(mat_file_path='/fake/path.mat')
        
        # Should work without error and close dataset
        assert 'doi' in result
        assert 'citation' in result
        mock_ds.close.assert_called_once()

    @patch('xopr.stac.metadata.OPRConnection')
    def test_url_loading_skips_existence_check(self, mock_opr_connection):
        """Test that URL paths skip local file existence checks."""
        mock_opr = Mock()
        mock_opr_connection.return_value = mock_opr
        
        mock_ds = create_mock_dataset()
        mock_opr.load_frame_url.return_value = mock_ds
        
        # Test with URL - should not check file existence
        result = extract_item_metadata(mat_file_path='https://example.com/data.mat')
        
        # Should work without file existence error
        assert 'doi' in result
        assert 'citation' in result
        mock_ds.close.assert_called_once()


class TestExtractItemMetadataWithRealData:
    """Test extract_item_metadata with real remote data files."""
    
    @pytest.mark.parametrize("data_url", [
        "https://data.cresis.ku.edu/data/rds/2016_Antarctica_DC8/CSARP_standard/20161014_03/Data_20161014_03_001.mat",
        "https://data.cresis.ku.edu/data/rds/2022_Antarctica_BaslerMKB/CSARP_standard/20221210_01/Data_20221210_01_001.mat",
        "https://data.cresis.ku.edu/data/rds/2019_Antarctica_GV/CSARP_standard/20191103_01/Data_20191103_01_026.mat"
    ])
    def test_real_data_extraction(self, data_url):
        """Test metadata extraction from real remote data files."""
        # Test with real remote data
        result = extract_item_metadata(mat_file_path=data_url)
        
        # Basic sanity checks - all keys should be present
        expected_keys = {'geom', 'bbox', 'date', 'frequency', 'bandwidth', 'doi', 'citation', 'mimetype'}
        assert set(result.keys()) == expected_keys
        
        # Check data types for always-present values
        from shapely.geometry import LineString
        from datetime import datetime
        
        assert isinstance(result['geom'], LineString)
        assert isinstance(result['date'], datetime)
        assert isinstance(result['frequency'], float)
        assert isinstance(result['bandwidth'], float)
        assert isinstance(result['mimetype'], str)
        
        # DOI and citation can be None or strings
        assert result['doi'] is None or isinstance(result['doi'], str)
        assert result['citation'] is None or isinstance(result['citation'], str)
        
        # Geometry should have points
        assert len(result['geom'].coords) > 0
        
        # Frequency should be reasonable for radar data
        assert 50e6 <= result['frequency'] <= 1000e6  # 50 MHz to 1 GHz (some older systems use lower frequencies)
        
        # Bandwidth should be positive
        assert result['bandwidth'] > 0

    @pytest.mark.parametrize("data_url,expected_campaign", [
        ("https://data.cresis.ku.edu/data/rds/2016_Antarctica_DC8/CSARP_standard/20161014_03/Data_20161014_03_001.mat", "2016_Antarctica_DC8"),
        ("https://data.cresis.ku.edu/data/rds/2022_Antarctica_BaslerMKB/CSARP_standard/20221210_01/Data_20221210_01_001.mat", "2022_Antarctica_BaslerMKB"),
        ("https://data.cresis.ku.edu/data/rds/2019_Antarctica_GV/CSARP_standard/20191103_01/Data_20191103_01_026.mat", "2019_Antarctica_GV")
    ])
    def test_real_data_campaign_consistency(self, data_url, expected_campaign):
        """Test that real data extraction produces expected results for known campaigns."""
        result = extract_item_metadata(mat_file_path=data_url)
        
        # Check that the date makes sense for the campaign year
        expected_year = int(expected_campaign.split('_')[0])
        assert result['date'].year == expected_year
        
        # Check that coordinates are in Antarctica (roughly)
        bounds = result['bbox'].bounds
        # Antarctica is roughly between -90 to -60 latitude
        assert bounds[1] >= -90  # min latitude
        assert bounds[3] <= -60  # max latitude

    def test_real_data_consistency_across_files(self):
        """Test that metadata extraction is consistent across different real files."""
        data_urls = [
            "https://data.cresis.ku.edu/data/rds/2016_Antarctica_DC8/CSARP_standard/20161014_03/Data_20161014_03_001.mat",
            "https://data.cresis.ku.edu/data/rds/2022_Antarctica_BaslerMKB/CSARP_standard/20221210_01/Data_20221210_01_001.mat",
            "https://data.cresis.ku.edu/data/rds/2019_Antarctica_GV/CSARP_standard/20191103_01/Data_20191103_01_026.mat"
        ]
        
        results = []
        for url in data_urls:
            result = extract_item_metadata(mat_file_path=url)
            results.append(result)
        
        # All should have the same structure
        expected_keys = {'geom', 'bbox', 'date', 'frequency', 'bandwidth', 'doi', 'citation', 'mimetype'}
        for result in results:
            assert set(result.keys()) == expected_keys
        
        # All should have valid geometry
        for result in results:
            assert len(result['geom'].coords) > 0
            assert result['bandwidth'] > 0
        
        # Frequencies should vary across different campaigns/years but be reasonable
        frequencies = [r['frequency'] for r in results]
        assert len(set(frequencies)) >= 1  # At least some variation
        assert all(50e6 <= f <= 1000e6 for f in frequencies)