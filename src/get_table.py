import pandas as pd
from pg8000.native import literal, identifier


def get_table(table_name: str, conn, timestamp) -> pd.DataFrame | None:
    ''' Queries a table using a pg8000 connection and returns all rows
        where last_updated is after the passed timestamp.
        Captures the column names from the connection
        and uses pandas to create a data frame
        from the result and the columns.
        If result is empty, returns None. Otherwise, returns
        dataframe.

        Parameters:
            - table_name: str
            - conn: pg8000.native Connection
            - timestamp: datetime.dateime

        Return value:
            - pd.DataFrame | None
    '''
    try:
        str_timestamp = str(timestamp)
        query = f'''SELECT * FROM {identifier(table_name)}
        WHERE last_updated > {literal(str_timestamp)}::timestamp'''
        results = conn.run(query, table_name=table_name)
        if not results:
            return None
        column_names = [desc['name'] for desc in conn.columns]
        table_data_frame = pd.DataFrame(results, columns=column_names)
        return table_data_frame
    except Exception as e:
        raise e
    finally:
        conn.close()
