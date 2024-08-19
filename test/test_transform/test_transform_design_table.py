from src.transform.transform_design_table import transform_design_table
import pytest 
import pandas as pd
from datetime import datetime


def test_returns_dataframe():
    data = {
        "design_id": [1],  
        "created_at": [datetime.now()],  
        "last_updated": [datetime.now()],  
        "design_name": ["Design A"],  
        "file_location": ["/path/to/file_a"],  
        "file_name": ["file_a.txt"],  
    }
    design_df = pd.DataFrame(data)

    assert isinstance(transform_design_table(design_df), pd.DataFrame)


def test_returns_dataframe_with_columns_removed():
    data = {
        "design_id": [1],  
        "created_at": [datetime.now()],  
        "last_updated": [datetime.now()],  
        "design_name": ["Design A"],  
        "file_location": ["/path/to/file_a"],  
        "file_name": ["file_a.txt"],  
    }
    design_df = pd.DataFrame(data)
    
    transformed_data = {
        "design_id": [1],  
        "design_name": ["Design A"],  
        "file_location": ["/path/to/file_a"],  
        "file_name": ["file_a.txt"],  
    }
    
    transformed_df = pd.DataFrame(transformed_data)
    
    assert transform_design_table(design_df).equals(transformed_df)
    
    
def test_returns_new_data_frame():
    data = {
        "design_id": [1],
        "created_at": [datetime.now()],
        "last_updated": [datetime.now()],
        "design_name": ["Design A"],
        "file_location": ["/path/to/file_a"],
        "file_name": ["file_a.txt"],
    }
    design_df = pd.DataFrame(data)

    assert not transform_design_table(design_df).equals(design_df)
