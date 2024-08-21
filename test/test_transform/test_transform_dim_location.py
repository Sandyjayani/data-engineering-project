import pandas as pd
import pytest
import logging
from src.transform.transform_dim_location import transform_dim_location



@pytest.fixture
def mock_data():
    return {
        'address': pd.DataFrame({
            'address_id': [1, 2, 3],
            'address_line_1': ['123 Main St', '456 Elm St', '789 Oak St'],
            'address_line_2': ['Apt 1', None, 'Suite 3'],
            'district': ['Central', 'North', 'South'],
            'city': ['Cityville', 'Townsburg', 'Villageton'],
            'postal_code': ['12345', '67890', '54321'],
            'country': ['Country A', 'Country B', 'Country C'],
            'phone': ['123-456-7890', '987-654-3210', '555-555-5555']
        })
    }


@pytest.mark.it("should transform address data to dim_location format")
def test_transform_dim_location_success(mock_data, caplog):
    caplog.set_level(logging.INFO)

    result = transform_dim_location(mock_data)
    assert isinstance(result, pd.DataFrame)
    assert 'location_id' in result.columns
    assert 'address_id' not in result.columns
    assert list(result.columns) == ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    assert len(result) == 3
    assert "dim_location transformation completed successfully" in caplog.text


@pytest.mark.it("should handle missing required columns")
def test_transform_dim_location_missing_columns(mock_data, caplog):
    caplog.set_level(logging.INFO)

    del mock_data['address']['address_line_1']

    result = transform_dim_location(mock_data)

    assert result is None
    assert "Missing required columns" in caplog.text


@pytest.mark.it("should fill 'Unknown' for missing non-critical data")
def test_missing_non_critical_data(caplog):
    data = {
        'address': pd.DataFrame({
            'address_id': [1],
            'address_line_1': ['123 Main St'],
            'address_line_2': [None],
            'district': [None],
            'city': ['Cityville'],
            'postal_code': ['12345'],
            'country': ['Country A'],
            'phone': ['123-456-7890']
        })
    }
    caplog.set_level(logging.INFO)

    result = transform_dim_location(data)
    assert result is not None
    assert result['address_line_2'].iloc[0] == 'Unknown'
    assert result['district'].iloc[0] == 'Unknown'
    assert "dim_location transformation completed successfully" in caplog.text


@pytest.mark.it("should drop rows with null values in critical columns")
def test_null_values_in_critical_columns(caplog):
    data = {
        'address': pd.DataFrame({
            'address_id': [1, None],
            'address_line_1': ['123 Main St', None],
            'address_line_2': ['Apt 1', 'Suite 3'],
            'district': ['Central', 'South'],
            'city': ['Cityville', None],
            'postal_code': ['12345', '54321'],
            'country': ['Country A', 'Country C'],
            'phone': ['123-456-7890', None]
        })
    }
    caplog.set_level(logging.INFO)

    result = transform_dim_location(data)
    assert result is not None
    assert len(result) == 1
    assert "dim_location transformation completed successfully" in caplog.text


@pytest.mark.it("should return None when no address data is provided")
def test_no_address_data(caplog):
    data = {}
    caplog.set_level(logging.INFO)

    result = transform_dim_location(data)
    assert result is None
    assert "Address data not found in the provided data dictionary" in caplog.text


@pytest.mark.it("should handle an empty DataFrame without error")
def test_empty_dataframe(caplog):
    data = {'address': pd.DataFrame(columns=['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone'])}
    caplog.set_level(logging.INFO)

    result = transform_dim_location(data)
    assert result is not None
    assert result.empty
    assert "dim_location transformation completed successfully" in caplog.text
