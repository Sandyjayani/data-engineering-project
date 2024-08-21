from transform.transform_dim_counterparty import transform_counterparty_table
import pandas as pd
import logging
import pytest




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

def test_returns_dataframe():
    counterparty_df = pd.DataFrame({
        "counterparty_id": [1, 2],
        "counterparty_legal_name": ["A", "B"],
        "legal_address_id": [1, 2],
        "commercial_contact": ["A", "B"],
        "delivery_contact": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-01"],
        "last_updated": ["2020-01-01", "2020-01-01"]
    })
    
    address_df = pd.DataFrame({
        "address_id": [1, 2],
        "address_line_1": ["A", "B"],
        "address_line_2": ["A", "B"],
        "district": ["A", "B"],
        "city": ["A", "B"],
        "postal_code": ["A", "B"],
        "country": ["A", "B"],
        "phone": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-01"],
        "last_updated": ["2020-01-01", "2020-01-01"]
    })
    
    
    df_dict = {
        "counterparty": counterparty_df,
        "address": address_df
    }
    
    assert isinstance(transform_counterparty_table(df_dict), pd.DataFrame)
    
    
def test_returns_dataframe_with_joined_columns():
    counterparty_df = pd.DataFrame({
        "counterparty_id": [1, 3],
        "counterparty_legal_name": ["A", "B"],
        "legal_address_id": [1, 2],
        "commercial_contact": ["A", "B"],
        "delivery_contact": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-01"],
        "last_updated": ["2020-01-01", "2020-01-01"]
    })
    
    address_df = pd.DataFrame({
        "address_id": [1, 2],
        "address_line_1": ["A", "B"],
        "address_line_2": ["A", "B"],
        "district": ["A", "B"],
        "city": ["A", "B"],
        "postal_code": ["A", "B"],
        "country": ["A", "B"],
        "phone": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-01"],
        "last_updated": ["2020-01-01", "2020-01-01"]
    })
    
    df_dict = {
        "counterparty": counterparty_df,
        "address": address_df
    }
    
    transformed_df = pd.DataFrame({
        "counterparty_id": [1, 3],
        "counterparty_legal_name": ["A", "B"],
        "counterparty_legal_address_line_1": ["A", "B"],
        "counterparty_legal_address_line2": ["A", "B"],
        "counterparty_legal_district": ["A", "B"],
        "counterparty_legal_city": ["A", "B"],
        "counterparty_legal_postal_code": ["A", "B"],
        "counterparty_legal_country": ["A", "B"],
        "counterparty_legal_phone_number": ["A", "B"],
    })
    
    assert transformed_df.equals(transform_counterparty_table(df_dict))


def test_returns_data_frame_without_any_null_values():
    counterparty_df = pd.DataFrame({
        "counterparty_id": [1, 2],
        "counterparty_legal_name": ["A", "B"],
        "legal_address_id": [1, 2],
        "commercial_contact": ["A", "B"],
        "delivery_contact": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-01"],
        "last_updated": ["2020-01-01", "2020-01-01"]
    })

    address_df = pd.DataFrame({
        "address_id": [1, 2],
        "address_line_1": ["A", "B"],
        "address_line_2": ["A", "B"],
        "district": ["A", "B"],
        "city": ["A", "B"],
        "postal_code": ["A", "B"],
        "country": ["A", "B"],
        "phone": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-01"],
        "last_updated": ["2020-01-01", "2020-01-01"]
    })

    df_dict = {
        "counterparty": counterparty_df,
        "address": address_df
    }

    transformed_df = transform_counterparty_table(df_dict)
    assert not transformed_df.isnull().values.any()
    
def test_changes_to_transformed_dataframe_do_not_affect_original_data():
    counterparty_df = pd.DataFrame({
            "counterparty_id": [1, 2],
            "counterparty_legal_name": ["A", "B"],
            "legal_address_id": [1, 2],
            "commercial_contact": ["A", "B"],
            "delivery_contact": ["A", "B"],
            "created_at": ["2020-01-01", "2020-01-01"],
            "last_updated": ["2020-01-01", "2020-01-01"]
        })

    address_df = pd.DataFrame({
            "address_id": [1, 2],
            "address_line_1": ["A", "B"],
            "address_line_2": ["A", "B"],
            "district": ["A", "B"],
            "city": ["A", "B"],
            "postal_code": ["A", "B"],
            "country": ["A", "B"],
            "phone": ["A", "B"],
            "created_at": ["2020-01-01", "2020-01-01"],
            "last_updated": ["2020-01-01", "2020-01-01"]
        })

    df_dict = {
            "counterparty": counterparty_df,
            "address": address_df
        }
    transformed_df = transform_counterparty_table(df_dict)
    transformed_df["counterparty_legal_name"] = ["X", "Y"]
    
    assert transformed_df['counterparty_id'] is not counterparty_df['counterparty_id']
    assert not transformed_df['counterparty_legal_name'][0] == counterparty_df['counterparty_legal_name'][0]
 

