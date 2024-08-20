from src.transform.transform_location_table import transform_location_table
import pandas as pd

# address	address_id 
# address	address_line_1
# address	address_line_2 
# address	district 
# address	city 
# address	postal_code 
# address	country
# address	phone 


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
    
    

# def test_returns_dataframe_with_column_names_removed

# def test_returns_new_dataframe 