import pandas as pd
import os

if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger
else:
    from src.transform.setup_logger import setup_logger

def transform_currency(data_dict: dict[str, pd.DataFrame]) -> pd.DataFrame | None:
    """
    Transform the currency data from the input dictionary into a structured DataFrame.

    Args:
        data_dict (dict): A dictionary containing the input data. Expected to have
                        a 'currency' key with a DataFrame value.

    Returns:
        pd.DataFrame | None: A DataFrame containing the transformed currency data
                    with columns 'currency_id', 'currency_code', and 'currency_name'.
                Returns None if the input data is invalid or missing required columns.

    Raises:
        Exception: Re-raises any unexpected exceptions that occur during processing.

    Logging:
        - INFO: Logs the start and successful completion of the transformation.
        - ERROR: Logs when currency data is missing or required columns are not present.
        - WARNING: Logs any currency codes that don't have a corresponding name in the mapping.
    """

    logger = setup_logger("transform_dim_currency")

    try:
        logger.info("Starting transformation for dim_currency.")
        df: pd.DataFrame | None = data_dict.get('currency')

        if df is None:
            logger.error("Currency data not found in the provided data dictionary.")
            return None

        required_columns = ['currency_id', 'currency_code']
        if not all(column in df.columns for column in required_columns):
            logger.error(f"One or more required columns are missing: {required_columns}")
            return None

        df = df.dropna(subset=required_columns)


        currency_name_map = {
            'GBP': 'British Pound',
            'USD': 'US Dollar',
            'EUR': 'Euro'
        }

        df['currency_name'] = df['currency_code'].map(currency_name_map)
        if df['currency_name'].isnull().any():
            missing_codes = df[df['currency_name'].isnull()]['currency_code'].unique()
            logger.warning(f"Currency codes with missing names: {missing_codes}")

        df = df[['currency_id', 'currency_code', 'currency_name']]

        logger.info("Transformation for dim_currency completed successfully.")
        return df

    except Exception as e:
        logger.error(f"Error in transform_dim_currency: {str(e)}")
        raise
