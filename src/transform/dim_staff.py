import pandas as pd
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from transform.load_combined_tables import (
        load_combined_tables as load_from_bucket,
    )
    from setup_logger import setup_logger
else:
    from src.transform.load_combined_tables import (
        load_combined_tables as load_from_bucket,
    )
    from src.transform.setup_logger import setup_logger


def transform_staff(df_dict: dict) -> pd.DataFrame | None:
    """Takes dictionary of dataframes containing any new data for each table.
    If there is new staff data,
    merges this with new and/or existing department data*
    and returns a new data frame, else returns None.
    The returned dataframe has following following columns:

        "staff_id [int, not null]"
        "first_name [varchar, not null]"
        "last_name [varchar, not null]"
        "department_name [varchar, not null]"
        "location [varchar, not null]"
        "email_address [email address, not null]

    Data validation checks are performed against the
    department_name, location, and email_address columns.
    Any rows with invalid data will be logged by staff_id.

    *If required department rows are not in the department dataframe,
    reads department data from the transformation bucket."""

    logger = setup_logger("transform_staff")

    try:
        logger.info("Starting staff table transformation.")

        # gets staff and department data from passed dict if present
        staff_df = df_dict.get("staff")
        new_depart_data = df_dict.get("department")

        # if no new staff data, returns None
        if not isinstance(staff_df, pd.DataFrame):
            logger.info("No new staff data to transform.")
            return None

        # capture departments ids for which we need department data
        dept_ids = staff_df["department_id"].unique().tolist()

        if isinstance(new_depart_data, pd.DataFrame):
            # gets list of department ids to retrieve from new_depart_data
            deps_in_df = new_depart_data["department_id"].tolist()
            # then creates df with the required cols
            dep_data_from_df = new_depart_data[
                ["department_id", "department_name", "location"]
            ]
            dep_data_from_df = dep_data_from_df.query(f"department_id in {dept_ids}")
        else:
            deps_in_df = []

        # if the passed dep data doesn't have all the data we need,
        # use the helper function to retrieve data from the transform bucket
        if data_needed_from_bucket := not all(
            [dep_id in deps_in_df for dep_id in dept_ids]
        ):
            dep_data_from_bucket = load_from_bucket("department", bucket_type="ingest")
            dep_data_from_bucket = dep_data_from_bucket.drop_duplicates(
                subset="department_id", keep="last"
            )
            dep_data_from_bucket = dep_data_from_bucket[
                ["department_id", "department_name", "location"]
            ]

        # if passed dep data has all the data we need,
        # join with staff data, tidy and return
        if not data_needed_from_bucket:
            staff_df = pd.merge(
                staff_df,
                dep_data_from_df,
                how="left",
                on="department_id",
                validate="m:1",
            )
            dim_staff_df = staff_df[
                [
                    "staff_id",
                    "first_name",
                    "last_name",
                    "department_name",
                    "location",
                    "email_address",
                ]
            ]

        else:
            staff_df = pd.merge(
                staff_df,
                dep_data_from_bucket,
                how="left",
                on="department_id",
                validate="m:1",
            )
            dim_staff_df = staff_df[
                [
                    "staff_id",
                    "first_name",
                    "last_name",
                    "department_name",
                    "location",
                    "email_address",
                ]
            ]

        # # if data only required from transform bucket,
        # # join with staff data, tidy and return
        # if data_needed_from_bucket and not isinstance(new_depart_data, pd.DataFrame):
        #     staff_df = pd.merge(
        #         staff_df,
        #         dep_data_from_bucket,
        #         how="left",
        #         on="department_id",
        #         validate="m:1",
        #     )
        #     dim_staff_df = staff_df[
        #         [
        #             "staff_id",
        #             "first_name",
        #             "last_name",
        #             "department_name",
        #             "location",
        #             "email_address",
        #         ]
        #     ]

        # # if data required from transform bucket and passed data,
        # # concat dep data, join with staff data, tidy and return
        # if isinstance(new_depart_data, pd.DataFrame) and data_needed_from_bucket:
        #     full_dep_data = pd.concat(
        #         [dep_data_from_bucket, dep_data_from_df], ignore_index=True
        #     )
        #     staff_df = pd.merge(
        #         staff_df, full_dep_data, how="left", on="department_id", validate="m:1"
        #     )
        #     dim_staff_df = staff_df[
        #         [
        #             "staff_id",
        #             "first_name",
        #             "last_name",
        #             "department_name",
        #             "location",
        #             "email_address",
        #         ]
        #     ]

        logger.info("Transformation of staff table completed successfully.")
        validate_staff_data(dim_staff_df)
        return dim_staff_df

    except Exception as e:
        logger.error(f"Error during staff table transformation: {str(e)}")
        raise e


def validate_staff_data(df: pd.DataFrame):
    validation_df = df.copy(deep=True)
    logger = setup_logger("transform_staff")
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    validation_df["is_valid_email"] = validation_df["email_address"].str.match(
        email_pattern
    )
    expected_locations = ["Manchester", "Leeds"]
    expected_deps = [
        "Communications",
        "Dispatch",
        "Facilities",
        "Finance",
        "HR",
        "Production",
        "Purchasing",
    ]
    invalid_locations = validation_df.query(f"not location in {expected_locations}")
    invalid_deps = validation_df.query(f"not department_name in {expected_deps}")
    invalid_emails = validation_df.query("is_valid_email == False")
    if not invalid_emails.empty:
        logger.info(
            f"""Data validation issue: invalid email(s) for staff_id(s):
                {invalid_emails["staff_id"].tolist()}."""
        )
    if not invalid_locations.empty:
        logger.info(
            f"""Data validation issue: invalid location(s) for staff_id(s):
                {invalid_locations["staff_id"].tolist()}."""
        )
    if not invalid_deps.empty:
        logger.info(
            f"""Data validation issue: invalid department(s) for staff_id(s):
                {invalid_deps["staff_id"].tolist()}."""
        )
    return None
