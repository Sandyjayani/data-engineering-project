import pandas as pd
import numpy as np
from unittest.mock import patch
import pytest
from src.transform.dim_staff import (
    transform_staff as ts,
    validate_staff_data as vs,
)


@pytest.fixture
def test_df_dict():
    return {
        "staff": pd.read_csv("test/test_transform/test_data/test_staff.csv"),
        "department": pd.read_csv("test/test_transform/test_data/test_department.csv"),
    }


@pytest.fixture
def test_transformed_staff_df():
    return pd.read_csv("test/test_transform/test_data/test_staff_transformed_data.csv")


@pytest.fixture
def part_A_dep_data():
    return pd.read_csv("test/test_transform/test_data/test_department_part_A.csv")


@pytest.fixture
def part_B_dep_data():
    return pd.read_csv("test/test_transform/test_data/test_department_part_B.csv")


class TestTransformStaff:
    @pytest.mark.it("Returns dataframe if passed staff data")
    @patch("src.transform.dim_staff.load_from_bucket")
    def test_returns_df(self, mock_load_existing_transformation_df, test_df_dict):
        output = ts(test_df_dict)
        assert isinstance(output, pd.DataFrame)

    @pytest.mark.it("Returns None if no staff data")
    def test_returns_none(self, test_df_dict):
        del test_df_dict["staff"]
        assert ts(test_df_dict) is None

    @pytest.mark.it("Returns dataframe with the correct columns")
    def test_returns_df_with_correct_cols(self, test_df_dict):
        new_df = ts(test_df_dict)
        expected_cols = [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
        output_cols = list(new_df.columns)
        assert expected_cols == output_cols

    @pytest.mark.it("Returns new dataframe")
    @patch("src.transform.dim_staff.load_from_bucket")
    def test_returns_new_df(self, test_df_dict):
        orig_df = test_df_dict["staff"]
        output_df = ts(test_df_dict)
        assert output_df is not orig_df

    @pytest.mark.it(
        "Calls load_from_bucket if all required departments not in passed department data"
    )
    @patch("src.transform.dim_staff.load_from_bucket")
    def test_calls_load_from_bucket(
        self, mock_load_from_bucket, part_A_dep_data, part_B_dep_data, test_df_dict
    ):
        test_df_dict["department"] = part_A_dep_data
        mock_load_from_bucket.return_value = part_B_dep_data
        ts(test_df_dict)
        mock_load_from_bucket.assert_called_once()

    @pytest.mark.it("Returns transformed df if no department data is passed in")
    @patch("src.transform.dim_staff.load_from_bucket")
    def test_returns_df_if_not_passed_dep_data(
        self, mock_load_from_bucket, test_df_dict
    ):
        del test_df_dict["department"]
        mock_load_from_bucket.return_value = pd.read_csv(
            "test/test_transform/test_data/test_department.csv"
        )
        output_df = ts(test_df_dict)
        expected_cols = [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
        output_cols = list(output_df.columns)
        assert expected_cols == output_cols

    @pytest.mark.it("Return df has no NaNs")
    def test_returned_df_has_no_nans(self, test_df_dict):
        output_df = ts(test_df_dict)
        output_deps = output_df["department_name"].tolist()
        outout_locations = output_df["location"].tolist()
        assert np.nan not in output_deps
        assert np.nan not in outout_locations


class TestValidateStaffData:
    @pytest.mark.it("Returns None")
    def returns_none(self, test_transformed_staff_df):
        assert vs(test_transformed_staff_df) is None

    @pytest.mark.it("Logs staff ids where location is invalid")
    def test_log_invalid_locations(self, test_transformed_staff_df, caplog):
        vs(test_transformed_staff_df)
        assert "[7, 8, 9]" in caplog.text

    @pytest.mark.it("Logs staff ids where department is invalid")
    def test_log_invalid_departments(self, test_transformed_staff_df, caplog):
        vs(test_transformed_staff_df)
        assert "[5, 6, 7]" in caplog.text

    @pytest.mark.it("Logs staff ids where department is invalid")
    def test_log_invalid_emails(self, test_transformed_staff_df, caplog):
        vs(test_transformed_staff_df)
        assert "[9]" in caplog.text
