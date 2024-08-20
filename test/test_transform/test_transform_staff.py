import pandas as pd
from unittest.mock import Mock, patch
import pytest
from src.transform.transform_staff import transform_staff as ts


@pytest.fixture
def test_df_dict():
    return {'staff': pd.read_csv('test/test_transform/test_staff.csv'),
            'department': pd.read_csv('test/test_transform/test_dep.csv')}
     

@pytest.mark.it('Returns dataframe if passed staff data')
@patch('src.transform.load_existing_transformation_df')
def test_returns_df(mock_load_existing_transformation_df, test_df_dict):
    output = ts(test_df_dict)
    assert isinstance(output, pd.DataFrame)


@pytest.mark.it('Returns dataframe with the correct columns')
@patch('src.transform.load_existing_transformation_df')
def test_returns_df_with_correct_cols(mock_load_existing_transformation_df, test_df_dict):
        new_df = ts(test_df_dict)
        expected_cols = ["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]
        output_cols = list(new_df.columns)
        assert expected_cols == output_cols


@pytest.mark.it('Returns new dataframe')
@patch('src.transform.load_existing_transformation_df')
def test_returns_new_df(mock_load_existing_transformation_df, test_df_dict):
    orig_df = test_df_dict
    output_df = ts(test_df_dict)
    assert output_df is not orig_df