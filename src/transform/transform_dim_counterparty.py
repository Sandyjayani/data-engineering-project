import pandas as pd

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
    
    ad_df = df_dict.get('address')
    address_df = ad_df.copy(deep=True)
    
    
    cp_df = df_dict.get('counterparty')
    counterparty_df = cp_df.copy(deep=True)
    
    combined_df = pd.merge(counterparty_df, address_df, left_on='legal_address_id', right_on='address_id', how='left')
    
    removed_columns = ['legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated', 'address_id']
    existing_columns_to_drop = [col for col in removed_columns if col in combined_df.columns]
    combined_df.drop(existing_columns_to_drop, axis=1, inplace=True)
    
    column_order = [
        'counterparty_id',
        'counterparty_legal_name',
        'address_line_1',
        'address_line_2',
        'district',
        'city',
        'postal_code',
        'country',
        'phone']
    
    transformed_df = combined_df[column_order]
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
    
   
    return transformed_df