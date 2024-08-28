import pandas as pd
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
    from load_combined_tables import load_combined_tables
else:
    from src.transform.setup_logger import setup_logger
    from src.transform.load_combined_tables import load_combined_tables

"""
    Transforms the 'counterparty' data from a given dictionary into a pandas DataFrame with specific requirements.

    This function retrieves a DataFrame associated with the 'counterparty' key from the input dictionary and performs
    several transformations to ensure data integrity and structure. It checks for the presence of required columns,
    handles missing values, and logs the process.

    Parameters:
    - df_dict (dict): A dictionary containing the data, 
      expected to have keys 'counterparty' and 'address' with a DataFrame as its value.

    Returns:
    - pd.DataFrame | None: A DataFrame with the required columns if successful 
      or None if the 'counterparty' data is not found or if required columns are missing.

    Raises:
    - Exception: For any other errors encountered during the transformation process, which are also logged.

    Logging:
    - Logs the start and successful completion of the transformation process. 
    - Logs if no new 'counterparty' data.
    """


def transform_counterparty(df_dict: dict):
    try:
        logger = setup_logger("transform_dim_counterparty")
        logger.info("Starting transform counterparty")

        cp_df = df_dict.get("counterparty")

        if cp_df is None:
            logger.info("No new address data to transform.")
            return None

        address_df = load_combined_tables("address", "ingest")

        counterparty_df = cp_df.copy(deep=True)

        combined_df = pd.merge(
            counterparty_df,
            address_df,
            left_on="legal_address_id",
            right_on="address_id",
            how="left",
            validate="m:1",
        )

        columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

        transformed_df = combined_df[columns]

        # Null values are acceptable in the schema, so we don't necessarily need this functionality.
        # Commented out, but can be reinstated if needed.

        # for column in transformed_df.columns:
        #     if transformed_df[column].isnull().any():
        #         if column == "counterparty_id":
        #             transformed_df = transformed_df.dropna(subset=[column])
        #         else:
        #             transformed_df.fillna({column: "Unknown"}, inplace=True)

        transformed_df.columns = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]

        logger.info("dim_counterparty transformation completed successfully")

        return transformed_df

    except Exception as e:
        logger.error(f"Error in transform_counterparty: {str(e)}")
        raise e
