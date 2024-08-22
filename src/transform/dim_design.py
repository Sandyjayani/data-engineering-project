import pandas as pd
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger  # type: ignore
else:
    from src.transform.setup_logger import setup_logger


def transform_design(data_dict: dict) -> pd.DataFrame | None:
    """
    Transforms the 'design' data from a given dictionary into a pandas DataFrame with specific requirements.

    This function retrieves a DataFrame associated with the 'design' key from the input dictionary and performs
    several transformations to ensure data integrity and structure. It checks for the presence of required columns,
    handles missing values, and logs the process.

    Parameters:
    - data_dict (dict): A dictionary containing the data, expected to have a key 'design' with a DataFrame as its value.

    Returns:
    - pd.DataFrame | None: A DataFrame with the required columns if successful, or None if the 'design' data is not found or if required columns are missing.

    Raises:
    - ValueError: If required columns are missing from the 'design' DataFrame.
    - Exception: For any other errors encountered during the transformation process, which are also logged.

    Logging:
    - Logs the start and successful completion of the transformation process.
    - Logs an error if the 'design' data is missing or if any other exception occurs.
    """

    logger = setup_logger("transform_dim_location")

    try:
        logger.info("Starting transformation for dim_location.")
        df = data_dict.get("design")
        if df is None:
            logger.error("Design data not found in the provided data dictionary.")
            return None

        required_columns = ["design_id", "design_name", "file_location", "file_name"]

        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            return None

        # Drop rows with missing 'design_id' and fill other missing values with 'Unknown' to ensure data integrity.
        for column in required_columns:
            if df[column].isnull().any():
                if column == "design_id":
                    df = df.dropna(subset=[column])
                else:
                    # df[column].fillna('Unknown', inplace=True)
                    df.fillna({column: "Unknown"}, inplace=True)

        df = df[required_columns]

        logger.info("dim_design transformation completed successfully")
        return df

    except Exception as e:
        logger.error(f"Error in transform_dim_design: {str(e)}")
        raise
