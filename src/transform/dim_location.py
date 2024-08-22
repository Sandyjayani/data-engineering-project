import pandas as pd
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger  # type: ignore
else:
    from src.transform.setup_logger import setup_logger

<<<<<<< HEAD:src/transform/transform_dim_location.py

def transform_dim_location(data_dict: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
=======
def transform_location(data_dict: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
>>>>>>> main:src/transform/dim_location.py
    """
    Transforms the 'address' DataFrame from the input dictionary into the 'dim_location' format.

    This function processes the 'address' DataFrame by:
    - Ensuring all required columns are present.
    - Dropping rows with missing values in critical columns.
    - Filling missing values in non-critical columns with 'Unknown'.
    - Renaming 'address_id' to 'location_id'.
    - Returning a DataFrame with the specified columns for 'dim_location'.

    Parameters:
    - data_dict (Dict[str, pd.DataFrame]): A dictionary containing the 'address' DataFrame.

    Returns:
    - Optional[pd.DataFrame]: A transformed DataFrame in the 'dim_location' format, or None if the transformation cannot be completed due to missing data or errors.
    """

    logger = setup_logger("transform_dim_location")

    try:
        logger.info("Starting transformation for dim_location.")
        df = data_dict.get("address")
        if df is None:
            logger.error("Address data not found in the provided data dictionary.")
            return None

        required_columns = [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

        # Find columns that are required but missing from the DataFrame
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return None

        # Handle missing data
        for column in required_columns:
            if df[column].isnull().any():
                if column in [
                    "address_id",
                    "address_line_1",
                    "city",
                    "postal_code",
                    "country",
                    "phone",
                ]:
                    df = df.dropna(subset=[column])
                else:
                    df[column].fillna("Unknown", inplace=True)

        df = df.rename(columns={"address_id": "location_id"})
        dim_location_columns = [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
        df = df[dim_location_columns]

        logger.info("dim_location transformation completed successfully")
        return df

    except Exception as e:
        logger.error(f"Error in transform_dim_location: {str(e)}")
        return None
