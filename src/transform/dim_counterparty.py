import pandas as pd
import os


if os.environ.get("AWS_EXECUTION_ENV"):
    from setup_logger import setup_logger # type: ignore
else:
    from src.extraction.setup_logger import setup_logger

"""
    Transforms the 'counterparty' data from a given dictionary into a pandas DataFrame with specific requirements.

    This function retrieves a DataFrame associated with the 'counterparty' key from the input dictionary and performs
    several transformations to ensure data integrity and structure. It checks for the presence of required columns,
    handles missing values, and logs the process.

    Parameters:
    - df_dict (dict): A dictionary containing the data, expected to have keys 'counterparty' and 'address' with a DataFrame as its value.

    Returns:
    - pd.DataFrame | None: A DataFrame with the required columns if successful, or None if the 'counterparty' data is not found or if required columns are missing.

    Raises:
    - ValueError: If required columns are missing from the 'counterparty' DataFrame. - ***todo***
    - Exception: For any other errors encountered during the transformation process, which are also logged.  ***todo***

    Logging:
    - Logs the start and successful completion of the transformation process.  ***todo***
    - Logs an error if the 'counterparty' data is missing or if any other exception occurs.  ***todo***
    """



def transform_counterparty_table(df_dict : dict):
    
    logger = setup_logger("transform_dim_counterparty")
    
    try:
        logger.info("Starting transformation for dim_counterparty")
        ad_df = df_dict.get('address')
        if ad_df is None:
            logger.error("address data not found in the provided data dictionary" )
            return None
        
        address_df = ad_df.copy(deep=True)
        
        
        cp_df = df_dict.get('counterparty')
        if cp_df is None:
            logger.error("counterparty data not found in the provided data dictionary" )
            return None
        
        counterparty_df = cp_df.copy(deep=True)
    
        combined_df = pd.merge(counterparty_df, address_df, left_on='legal_address_id', right_on='address_id', how='left')
        print('combined_df 1>>>>>>>>',combined_df.to_string())

        
        removed_columns = ['created_at_x','last_updated_x','legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at_y', 'last_updated_y', 'address_id']
        existing_columns_to_drop = [col for col in removed_columns if col in combined_df.columns]
        combined_df.drop(existing_columns_to_drop, axis=1, inplace=True)
        
        columns = [
            'counterparty_id',
            'counterparty_legal_name',
            'address_line_1',
            'address_line_2',
            'district',
            'city',
            'postal_code',
            'country',
            'phone']
        
        print('combined_df 2>>>>>>>>',combined_df.to_string())
        
        
        
        missing_columns = [col for col in columns if col not in combined_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns})")
            return None
       
        transformed_df = combined_df[columns]
        print('transformed_df >>>>>>>>', transformed_df.to_string())


        for column in transformed_df.columns:
            if transformed_df[column].isnull().any():
                if column == 'counterparty_id':
                    transformed_df = transformed_df.dropna(subset=[column])
                else:
                    transformed_df.fillna({column: 'Unknown'}, inplace=True)
                    
        transformed_df = transformed_df[columns]
        
         
        transformed_df.columns = [
            'counterparty_id', 
            'counterparty_legal_name', 
            'counterparty_legal_address_line_1', 
            'counterparty_legal_address_line2', 
            'counterparty_legal_district', 
            'counterparty_legal_city', 
            'counterparty_legal_postal_code', 
            'counterparty_legal_country', 
            'counterparty_legal_phone_number']
        
        logger.info("dim_counterparty transformation completed successfully")
        print('transformed_df >>>>>>>>', transformed_df.to_string())

        return transformed_df
        

    except Exception as e:
        logger.error(f"Error in transform_dim_counterparty: {str(e)}")
        raise