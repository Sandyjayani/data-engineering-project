import pandas as pd


# transform from...

# Table address {
#   address_id int [pk, increment, not null]
#   address_line_1 varchar [not null]
#   address_line_2 varchar [note: 'nullable']
#   district varchar [note: 'nullable']
#   city varchar [not null]
#   postal_code varchar [not null]
#   country varchar [not null]
#   phone varchar [not null, note: 'valid phone number']
#   created_at timestamp [not null, default: `current_timestamp`]
#   last_updated timestamp [not null, default: `current_timestamp`]
# }
# Table counterparty {
#   counterparty_id int [pk, increment, not null]
#   counterparty_legal_name varchar [not null]
#   legal_address_id int [not null, ref: > A.address_id]  -- *** join on this - check counterparty has correct city based on address id ***
#   commercial_contact varchar [note: 'person name, nullable']
#   delivery_contact varchar [note: 'person name, nullable']
#   created_at timestamp [not null, default: `current_timestamp`]
#   last_updated timestamp [not null, default: `current_timestamp`]
# }




# transform to dim_counterparty
# 	counterparty_id	                    NN	        primary key	int
# 	counterparty_legal_name 	        NN		    varchar
# 	counterparty_legal_address_line_1 	NN		    varchar
# 	counterparty_legal_address_line2 	optional	varchar
# 	counterparty_legal_district	        optional	varchar
# 	counterparty_legal_city 	        NN		    varchar
# 	counterparty_legal_postal_code 	    NN		    varchar
# 	counterparty_legal_country 	        NN		    varchar
# 	counterparty_legal_phone_number 	NN		    varchar



def transform_counterparty_table(df_dict : dict):
    
    ad_df = df_dict.get('address')
    address_df = ad_df.copy(deep=True)
    removed_address_columns = ['address_id','created_at','last_updated']
    address_df.drop(removed_address_columns, axis=1, inplace=True)
    
    
    cp_df = df_dict.get('counterparty')
    counterparty_df = cp_df.copy(deep=True)
    removed_counterparty_columns = ['legal_address_id','commercial_contact','delivery_contact','created_at','last_updated']
    counterparty_df.drop(removed_counterparty_columns, axis=1, inplace=True)
    
    # merge on legal_address_id == address_id ***
    combined_df = pd.concat([counterparty_df, address_df], axis=1)
    
    
    
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
    print("transformed",transformed_df)
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