import pandas as pd
import pytest
from src.transform.transform_staff import transform_staff as ts


@pytest.fixture
def test_df():
    test_df = pd.read_csv('test/test_transform/test_staff.csv')
    return test_df


@pytest.mark.it('Returns dataframe')
def test_returns_df(test_df):
    output = ts(test_df)
    assert isinstance(output, pd.DataFrame)


@pytest.mark.it('Returns dataframe with the correct columns')
def test_returns_df_with_correct_cols(test_df):
        new_df = ts(test_df)
        expected_cols = ["staff_id", "first_name", "last_name", "department_name", "location", "email_address"]
        output_cols = list(new_df.columns)
        assert expected_cols == output_cols


@pytest.mark.it('Returns new dataframe')
def test_returns_new_df(test_df):
    orig_df = test_df
    output_df = ts(test_df)
    assert output_df is not orig_df