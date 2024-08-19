import pandas as pd

# recive dataframe - latest updata of design table

# convert from ...
# Table design as D {
#   design_id int [pk, increment, not null]
#   created_at timestamp [not null, default: `current_timestamp`]       x
#   last_updated timestamp [not null, default: `current_timestamp`]     x
#   design_name varchar [not null]
#   file_location varchar [not null, note: 'directory location']
#   file_name varchar [not null, note: 'file name']
# }

# converts to....

# Table dim_design as D{
#   design_id int [pk, not null]
#   design_name varchar [not null]
#   file_location varchar [not null]
#   file_name varchar [not null]
# }

def transform_design_table(design_df):
    transformed_df = design_df.drop(columns=['created_at', 'last_updated'])
    return transformed_df
    
