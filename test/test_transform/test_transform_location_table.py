from src.transform.transform_location_table import transform_location_table
import pandas as pd

# transform from ....
# address	address_id 
# address	address_line_1
# address	address_line_2 
# address	district 
# address	city 
# address	postal_code 
# address	country
# address	phone 

# transform to

#   location_id int [pk, not null]
#   address_line_1 varchar [not null]
#   address_line_2 varchar
#   district varchar
#   city varchar [not null]
#   postal_code varchar [not null]
#   country varchar [not null]
#   phone varchar [not null]

def test_returns_data_frame():
    data = {
        "address_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": [None],
        "district": ["District A"],
        "city": ["City A"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["000000000000"],
    }
    
    location_df = pd.DataFrame(data)
    
    assert isinstance(transform_location_table(location_df), pd.DataFrame)
    
    

def test_returns_dataframe_with_column_names_removed():
    data = {
        "address_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": [None],
        "district": ["District A"],
        "city": ["City A"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["000000000000"],
    }
    address_df = pd.DataFrame(data)
    
    transformed_data = {'location_id': [1],
                        'address_line_1': ['123 Main St'],
                        'address_line_2': [None],
                        'district': ['District A'],
                        'city': ['City A'],
                        'postal_code': ['12345'],
                        'country': ['USA'],
                        'phone': ['000000000000']}
    transformed_df = pd.DataFrame(transformed_data)
    
    assert transform_location_table(address_df).equals(transformed_df)
    
 

def test_returns_new_dataframe():
    data = {
        "address_id": [1],
        "address_line_1": ["123 Main St"],
        "address_line_2": [None],
        "district": ["District A"],
        "city": ["City A"],
        "postal_code": ["12345"],
        "country": ["USA"],
        "phone": ["000000000000"],
    }
    address_df = pd.DataFrame(data)

    assert not transform_location_table(address_df).equals(address_df)