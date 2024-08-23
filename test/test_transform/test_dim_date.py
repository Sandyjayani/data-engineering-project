from src.transform.dim_date import dim_date
import pytest
import pandas as pd



@pytest.mark.it("should create a date dimension DataFrame with the correct structure")
def test_transform_dim_date():
    df = dim_date()

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
    assert df['date_id'].min() == pd.to_datetime('2022-01-01')
    assert df['date_id'].max() == pd.to_datetime('2024-12-31')
    assert df['year'].min() == 2022
    assert df['year'].max() == 2024