import pytest
import pandas as pd
import logging
from src.transform.dim_currency import transform_currency

@pytest.fixture
def mock_data():
    """Fixture to provide mock data for testing."""
    return {
        'currency': pd.DataFrame({
            'currency_id': [1, 2, 3],
            'currency_code': ['GBP', 'USD', 'EUR']
        })
    }



@pytest.fixture
def mock_data_missing_columns():
    """Fixture to provide mock data with missing columns."""
    return {
        'currency': pd.DataFrame({
            'currency_id': [1, 2, 3]
            # 'currency_code' column is missing
        })
    }

@pytest.fixture
def mock_data_missing_dataframe():
    """Fixture to provide mock data without the currency DataFrame."""
    return {}

@pytest.mark.it("should transform currency data and add currency_name")
def test_transform_dim_currency(mock_data):
    df = transform_currency(mock_data)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == ['currency_id', 'currency_code', 'currency_name']
    assert df['currency_name'].tolist() == ['British Pound', 'US Dollar', 'Euro']


@pytest.mark.it("should log an error if currency DataFrame is missing")
def test_transform_dim_currency_missing_dataframe(caplog, mock_data_missing_dataframe):
    caplog.set_level(logging.INFO)

    df = transform_currency(mock_data_missing_dataframe)

    assert df is None
    assert "Currency data not found in the provided data dictionary." in caplog.text


@pytest.mark.it("should log an error if required columns are missing")
def test_transform_dim_currency_missing_columns(caplog, mock_data_missing_columns):
    caplog.set_level(logging.INFO)


    df = transform_currency(mock_data_missing_columns)
    assert df is None
    assert "One or more required columns are missing" in caplog.text


@pytest.mark.it("should log a warning if currency codes are missing in the mapping")
def test_transform_dim_currency_missing_mapping(caplog, mock_data):
    caplog.set_level(logging.INFO)

    new_row = pd.DataFrame({'currency_id': [4], 'currency_code': ['XYZ']})
    mock_data['currency'] = pd.concat([mock_data['currency'], new_row], ignore_index=True)

    df = transform_currency(mock_data)

    assert "Currency codes with missing names: ['XYZ']" in caplog.text

    assert df is not None
    assert df['currency_name'].isnull().sum() == 1

