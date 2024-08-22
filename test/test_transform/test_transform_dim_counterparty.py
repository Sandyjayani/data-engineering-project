from src.transform.transform_dim_counterparty import transform_counterparty_table
import pandas as pd
import logging
import pytest

@pytest.fixture
def valid_address_data():
    return  pd.DataFrame({
        "address_id": [1, 2],
        "address_line_1": ["123 Main St", "456 Oak Ave"],
        "address_line_2": ["Suite 100", "Suite 200"],
        "district": ["District A", "District B"],
        "city": ["City A", "City B"],
        "postal_code": ["12345", "67890"],
        "country": ["USA", "Canada"],
        "phone": ["000000000000", "000000000000"],
        "created_at": ["2020-01-01", "2020-01-02"],
        "last_updated": ["2020-01-01", "2020-01-02"]
    })
    
@pytest.fixture
def valid_counterparty_data():
    return  pd.DataFrame({
        "counterparty_id": [1, 2],
        "counterparty_legal_name": ["A", "B"],
        "legal_address_id": [1, 2],
        "commercial_contact": ["A", "B"],
        "delivery_contact": ["A", "B"],
        "created_at": ["2020-01-01", "2020-01-02"],
        "last_updated": ["2020-01-01", "2020-01-02"]
    })
    
@pytest.fixture
def valid_df_dict(valid_address_data, valid_counterparty_data):
    return {'counterparty': valid_counterparty_data, 'address': valid_address_data}
    
@pytest.fixture
def expected_output():
    return  pd.DataFrame({
        "counterparty_id": [1, 2],
        "counterparty_legal_name": ["A", "B"],
        "counterparty_legal_address_line_1": ["123 Main St", "456 Oak Ave"],
        "counterparty_legal_address_line2": ["Suite 100", "Suite 200"],
        "counterparty_legal_district": ["District A", "District B"],
        "counterparty_legal_city": ["City A", "City B"],
        "counterparty_legal_postal_code": ["12345", "67890"],
        "counterparty_legal_country": ["USA", "Canada"],
        "counterparty_legal_phone_number": ["000000000000", "000000000000"]
    })
    



@pytest.mark.it("transforms valid counterparty data successfully")
def test_transform_dim_counterparty_successfully(valid_df_dict,expected_output, caplog):
    with caplog.at_level(logging.INFO):
        transformed_df = transform_counterparty_table(valid_df_dict)
        
        assert isinstance(transformed_df, pd.DataFrame) # transformed df is a dataframe
        assert transformed_df.equals(expected_output)   # transformed df has correct column layout
        assert not transformed_df.isnull().values.any() # transformed data has no null values
        assert "dim_design transformation completed successfully" in caplog.text



@pytest.mark.it("transforms valid counterparty data successfully without affecting original data")
def test_transform_dim_counterparty_changes_do_not_affect_original_data(valid_df_dict,valid_counterparty_data,caplog):
    with caplog.at_level(logging.INFO):
        transformed_df = transform_counterparty_table(valid_df_dict)
        transformed_df["counterparty_legal_name"] = ["X", "Y"]
        
        assert transformed_df['counterparty_id'] is not valid_counterparty_data['counterparty_id']
        assert not transformed_df['counterparty_legal_name'][0] == valid_counterparty_data['counterparty_legal_name'][0]
        assert "dim_design transformation completed successfully" in caplog.text

    

