import pandas as pd
import os
if os.environ.get("AWS_EXECUTION_ENV"):
    from load_existing_transformation_df import load_existing_transformation_df
else:
    from src.transform.load_existing_transformation_df import load_existing_transformation_df



def transform_staff(df_dict: dict) -> pd.DataFrame | None:
    """Takes dictionary of dataframes containing any new data for each table. 
    Joins new staff data with new and/or existing department data and returns a data frame
    with the following following columns:
    
        "staff_id [int, not null]" 
        "first_name [varchar, not null]" 
        "last_name [varchar, not null]" 
        "department_name [varchar, not null]" 
        "location [varchar, not null]"
        "email_address [email address, not null]
    
    If required department rows are not in the department dataframe, 
    reads department data from the transformation bucket."""

    # gets staff and department data from passed dict if present
    orig_staff_df = df_dict.get('staff')
    new_depart_data = df_dict.get('department')

    # if no new staff data, returns None
    if not orig_staff_df:
        return None

    # remove uneeded cols from staff_df
    staff_df = orig_staff_df.drop(columns=['created_at', 'last_updated'])

    # capture departments ids for which we need department data
    dept_ids = staff_df['department_id'].unique().tolist()
    
    if new_depart_data:
        # gets list of department ids to retrieve from new_depart_data
        deps_in_df = new_depart_data['department_id'].tolist()
        # then creates df with the required cols
        dep_data_from_df = new_depart_data[["department_id", "department_name", "location"]]
        dep_data_from_df = dep_data_from_df.query(f"department_id in {dept_ids}")
    else:
        deps_in_df = []

    # if the passed dep data doesn't have all the data we need, use the helper function to retrieve data from the transform bucket
    if (data_needed_from_bucket := not all([dep_id in deps_in_df for dep_id in dept_ids])):
        dep_data_from_bucket = load_existing_transformation_df('department')
        dep_data_from_bucket = dep_data_from_df[["department_id", "department_name", "location"]]
    
    # if passed dep data has all the data we need, join with staff data, tidy and return
    if new_depart_data and not data_needed_from_bucket:
        staff_df = staff_df.join(dep_data_from_df, on="department_id", how="left", lsuffix='XX', validate="m:1")
        staff_df.drop(columns=['department_idXX'], inplace=True)
        dim_staff_df = staff_df.reindex(columns=[col for col in staff_df.columns if col != 'email_address'] + ['email_address'])
        return dim_staff_df
    
    # if data only required from bucket, join with staff data, tidy and return
    if data_needed_from_bucket and not new_depart_data:
        staff_df = staff_df.join(dep_data_from_bucket, on="department_id", how="left", lsuffix='XX', validate="m:1")
        staff_df.drop(columns=['department_idXX'], inplace=True)
        dim_staff_df = staff_df.reindex(columns=[col for col in staff_df.columns if col != 'email_address'] + ['email_address'])
        return dim_staff_df

    # if data required from bucket and passed data, concat dep data, join with staff data, tidy and return
    if new_depart_data and data_needed_from_bucket:
        full_dep_data = pd.concat([dep_data_from_bucket, dep_data_from_df], ignore_index=True)
        staff_df = staff_df.join(full_dep_data, on="department_id", how="left", lsuffix='XX', validate="m:1")
        staff_df.drop(columns=['department_idXX'], inplace=True)
        dim_staff_df = staff_df.reindex(columns=[col for col in staff_df.columns if col != 'email_address'] + ['email_address'])
        return dim_staff_df

