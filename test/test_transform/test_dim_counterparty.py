from src.transform.dim_counterparty import transform_counterparty_table
import pandas as pd
import logging
import pytest

@pytest.fixture
def valid_address_data():
    return  pd.DataFrame({
        'address_id': [1, 2, 3, 4, 5],
        'address_line_1': ['123 Main St', '456 Oak Ave', '789 Elm St', '101 Pine St', '202 Cedar St'],
        'address_line_2': ['Suite 100', 'Suite 200', 'Suite 300', 'Suite 400', 'Suite 500'],
        'district': ['District A', 'District B', 'District C', 'District D', 'District E'],
        'city': ['City A', 'City B', 'City C', 'City D', 'City E'],
        'postal_code': ['12345', '67890', '12345', '67890', '12345'],
        'country': ['USA', 'Canada', 'USA', 'Canada', 'USA'],
        'phone': ['123456789', '987654321', '123456789', '987654321', '123456789'],
        'created_at': ['2020-01-01', '2020-01-02', '2020-01-01', '2020-01-02', '2020-01-01'],
        'last_updated': ['2020-01-01', '2020-01-02', '2020-01-01', '2020-01-02', '2020-01-01']

        
    })
    
@pytest.fixture
def valid_counterparty_data():
    return  pd.DataFrame({
        'counterparty_id': [1, 2, 3, 4, 5],
        'counterparty_legal_name': ['A', 'B', 'C', 'D', 'E'],
        'legal_address_id': [1, 2, 3, 4, 5],
        'commercial_contact': ['A', 'B', 'C', 'D', 'E'],
        'delivery_contact': ['A', 'B', 'C', 'D', 'E'],
        'created_at': ['2020-01-01', '2020-01-02', '2020-01-01', '2020-01-02', '2020-01-01'],
        'last_updated': ['2020-01-01', '2020-01-02', '2020-01-01', '2020-01-02', '2020-01-01']
        
    })
    
@pytest.fixture
def valid_df_dict(valid_address_data, valid_counterparty_data):
    return {'counterparty': valid_counterparty_data, 'address': valid_address_data}
    
@pytest.fixture
def expected_output():
    return  pd.DataFrame({
     
        'counterparty_id': [1, 2, 3, 4, 5],
        'counterparty_legal_name': ['A', 'B', 'C', 'D', 'E'],
        'counterparty_legal_address_line_1': ['123 Main St', '456 Oak Ave', '789 Elm St', '101 Pine St', '202 Cedar St'],
        'counterparty_legal_address_line_2': ['Suite 100', 'Suite 200', 'Suite 300', 'Suite 400', 'Suite 500'],
        'counterparty_legal_district': ['District A', 'District B', 'District C', 'District D', 'District E'],
        'counterparty_legal_city': ['City A', 'City B', 'City C', 'City D', 'City E'],
        'counterparty_legal_postal_code': ['12345', '67890', '12345', '67890', '12345'],
        'counterparty_legal_country': ['USA', 'Canada', 'USA', 'Canada', 'USA'],
        'counterparty_legal_phone_number': ['123456789', '987654321', '123456789', '987654321', '123456789']
   
    })
    
@pytest.fixture
def missing_column_df_dict():
    return {'counterparty': pd.DataFrame({
        "counterparty_id": [1, 2],
        "counterparty_legal_name": ["A", "B"],
        "legal_address_id": [1, 2],
        "commercial_contact": ["A", "B"],
        "delivery_contact": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-02"],
        "last_updated": ["2020-01-01", "2020-01-02"]
    }), 'address': pd.DataFrame({
        "address_id": [1, 2],
        # "address_line_1": ["123 Main St", "456 Oak Ave"], address_line_1 missing
        "address_line_2": ["Suite 100", "Suite 200"],
        "district": ["District A", "District B"],
        "city": ["City A", "City B"],
        "postal_code": ["12345", "67890"],
        "country": ["USA", "Canada"],
        "phone": ["123456789", "987654321"]
    })}
    



@pytest.mark.it("transforms valid counterparty data successfully")
def test_transform_dim_counterparty_successfully(valid_df_dict,expected_output, caplog):
    with caplog.at_level(logging.INFO):
        transformed_df = transform_counterparty_table(valid_df_dict)
        
        assert isinstance(transformed_df, pd.DataFrame) # transformed df is a dataframe
        assert transformed_df.equals(expected_output)   # transformed df has correct column layout
        assert not transformed_df.isnull().values.any() # transformed data has no null values
        assert "dim_counterparty transformation completed successfully" in caplog.text



@pytest.mark.it("transforms valid counterparty data successfully without affecting original data")
def test_transform_dim_counterparty_changes_do_not_affect_original_data(valid_df_dict,valid_counterparty_data,caplog):
    with caplog.at_level(logging.INFO):
        transformed_df = transform_counterparty_table(valid_df_dict)
        transformed_df["counterparty_legal_name"] = ["X", "Y", "Z", "W", "V"] 
        
        assert transformed_df['counterparty_id'] is not valid_counterparty_data['counterparty_id']
        assert not transformed_df['counterparty_legal_name'][0] == valid_counterparty_data['counterparty_legal_name'][0]
        assert "dim_counterparty transformation completed successfully" in caplog.text
        

    
@pytest.mark.it("should log an error and return None when required columns are missing")
def test_transform_dim_counterparty_missing_columns(missing_column_df_dict, caplog):
    with caplog.at_level(logging.ERROR):
        transformed_df = None
        try:
            transformed_df = transform_counterparty_table(missing_column_df_dict)
        except ValueError:
            pass
        assert transformed_df is None
        assert "Missing required columns" in caplog.text
        
        
@pytest.mark.it("should handle missing values by filling with 'Unknown")
def test_transform_dim_counterparty_missing_values(valid_df_dict, expected_output, caplog):
    with caplog.at_level(logging.INFO):
        valid_df_dict['address'].loc[0,'address_line_1'] = None
        transformed_df = transform_counterparty_table(valid_df_dict)
        
        assert isinstance(transformed_df, pd.DataFrame) # transformed df is a dataframe
        assert len(transformed_df) == len(expected_output) # transformed df has correct number of rows
        assert transformed_df.loc[0,"counterparty_legal_address_line_1"] == 'Unknown'
        assert "dim_counterparty transformation completed successfully" in caplog.text
        

@pytest.mark.it("should handle missing values by dropping rows with missing 'counterparty_id'")
def test_transform_dim_counterparty_missing_counterparty_id(valid_df_dict, expected_output, caplog):
    with caplog.at_level(logging.INFO):
        valid_df_dict['counterparty'].loc[0,'counterparty_id'] = None
        transformed_df = transform_counterparty_table(valid_df_dict)

        assert isinstance(transformed_df, pd.DataFrame) # transformed df is a dataframe
        assert len(transformed_df) == len(expected_output) - 1 # transformed df has correct number of rows
        assert "dim_counterparty transformation completed successfully" in caplog.text
        
@pytest.mark.it("should log an error and raise an exception for unexpected errors")
def test_transform_dim_counterparty_unexpected_error(caplog):
    with caplog.at_level(logging.ERROR):
        data_dict = {'counterparty': "This is not a DataFrame"}
        with pytest.raises(Exception):
            transform_counterparty_table(data_dict)
        assert "address data is missing or is not a DataFrame" in caplog.text
        
@pytest.mark.it("should merge the correct address id by legal address id")
def test_transform_dim_counterparty_merge_correct_address_id(valid_df_dict, expected_output, caplog):
    with caplog.at_level(logging.INFO):
        transformed_df = transform_counterparty_table(valid_df_dict)
        for i in range(len(transformed_df)):
            assert transformed_df.loc[i,"counterparty_legal_address_line_1"] == expected_output.loc[i,"counterparty_legal_address_line_1"]
        
            
        
      
        assert "dim_counterparty transformation completed successfully" in caplog.text