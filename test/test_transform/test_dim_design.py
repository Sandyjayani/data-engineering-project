import pytest
import pandas as pd
import logging
from src.transform.dim_design import transform_design


@pytest.fixture
def valid_data():
    return {
        "design": pd.DataFrame(
            {
                "design_id": [1, 2, 3],
                "design_name": ["Design A", "Design B", "Design C"],
                "file_location": ["/path/a", "/path/b", "/path/c"],
                "file_name": ["file_a.json", "file_b.json", "file_c.json"],
            }
        )
    }


@pytest.fixture
def missing_column_data():
    return {
        "design": pd.DataFrame(
            {
                "design_name": ["Design A", "Design B", "Design C"],
                "file_location": ["/path/a", "/path/b", "/path/c"],
                "file_name": ["file_a.json", "file_b.json", "file_c.json"],
            }
        )
    }


@pytest.fixture
def data_with_missing_values():
    return {
        "design": pd.DataFrame(
            {
                "design_id": [1, 2, 3],
                "design_name": ["Design A", None, "Design C"],
                "file_location": ["/path/a", "/path/b", None],
                "file_name": ["file_a.json", "file_b.json", "file_c.json"],
            }
        )
    }


@pytest.fixture
def data_with_missing_design_id():
    return {
        "design": pd.DataFrame(
            {
                "design_id": [1, None, 3],
                "design_name": ["Design A", "Design B", "Design C"],
                "file_location": ["/path/a", "/path/b", "/path/c"],
                "file_name": ["file_a.json", "file_b.json", "file_c.json"],
            }
        )
    }


@pytest.mark.it("should transform valid design data successfully")
def test_transform_dim_design_success(valid_data, caplog):
    with caplog.at_level(logging.INFO):
        result = transform_design(valid_data)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == [
            "design_id",
            "design_name",
            "file_location",
            "file_name",
        ]
        assert len(result) == 3
        assert "dim_design transformation completed successfully" in caplog.text


@pytest.mark.it("should log an error and return None when required columns are missing")
def test_transform_dim_design_missing_columns(missing_column_data, caplog):
    with caplog.at_level(logging.ERROR):
        result = None
        try:
            result = transform_design(missing_column_data)
        except ValueError:
            pass
        assert result is None
        assert "Missing required columns" in caplog.text


@pytest.mark.it("should handle missing values by filling with 'Unknown'")
def test_transform_dim_design_missing_values(data_with_missing_values, caplog):
    with caplog.at_level(logging.INFO):
        result = transform_design(data_with_missing_values)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

        assert (
            result.loc[result["design_id"] == 2, "design_name"].values[0] == "Unknown"
        )
        assert (
            result.loc[result["design_id"] == 3, "file_location"].values[0] == "Unknown"
        )

        assert "dim_design transformation completed successfully" in caplog.text


@pytest.mark.it("should handle missing values by dropping rows with missing 'design_id")
def test_transform_dim_design_missing_design_id(data_with_missing_design_id, caplog):
    with caplog.at_level(logging.INFO):
        result = transform_design(data_with_missing_design_id)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

        assert "dim_design transformation completed successfully" in caplog.text


@pytest.mark.it("should log an error and raise an exception for unexpected errors")
def test_transform_dim_design_unexpected_error(caplog):
    # Simulate an unexpected error by passing an invalid type
    data_dict = {"design": "This is not a DataFrame"}
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            transform_design(data_dict)
        assert "Error in transform_dim_design" in caplog.text
