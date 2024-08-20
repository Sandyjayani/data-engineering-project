import pandas as pd

# transform from....

# address	address_id 
# address	address_line_1
# address	address_line_2 
# address	district 
# address	city 
# address	postal_code 
# address	country
# address	phone 


# transform to....


# Table dim_location as LOC {
#   location_id int [pk, not null]
#   address_line_1 varchar [not null]
#   address_line_2 varchar
#   district varchar
#   city varchar [not null]
#   postal_code varchar [not null]
#   country varchar [not null]
#   phone varchar [not null]
# }

def transform_location_table(address_df):
    return address_df