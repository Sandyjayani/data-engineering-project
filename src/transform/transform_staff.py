import pandas as pd
from src.extraction.get_db_connection import create_connection


def transform_staff(orig_df, ):
    dept_ids = orig_df['department_id'].unique().tolist()
    department_data = get_department_data(dept_ids)
    new_df = orig_df.copy(deep=True)
    new_df = new_df.join(department_data, on="department_id", how="left", lsuffix='XX', validate="m:1")
    new_df = new_df.drop(columns=['department_idXX', 'created_at', 'last_updated', 'department_id'])
    new_df = new_df.reindex(columns=[col for col in new_df.columns if col != 'email_address'] + ['email_address'])
    return new_df



def get_department_data(dept_ids: list):
    dept_ids = tuple(dept_ids)
    try:
        conn = create_connection()
        query = f"""SELECT department_id, department_name, location FROM department 
                    WHERE department_id in {dept_ids}"""
        result = conn.run(query)
        cols = [col['name'] for col in conn.columns]
        department_df = pd.DataFrame(result, columns=cols)
        return department_df
    finally:
        if "conn" in locals():
            conn.close()

