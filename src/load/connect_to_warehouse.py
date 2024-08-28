import os

if os.environ.get("AWS_EXECUTION_ENV") is not None:
    from get_db_connection import create_connection
else:
    from src.load.get_db_connection import create_connection


def have_a_look_at_the_warehouse():
    db = create_connection("load")
    are_there_tables_query = """
    SELECT table_schema , table_name 
    FROM information_schema.tables
    WHERE table_schema not in ('information_schema', 'pg_catalog')
        AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
    """  # the answer was yes
    sql_query = """
    SELECT * from project_team_9.fact_sales_order;
    """
    response = db.run(sql_query)
    columns = db.columns
    print(response, columns)


if __name__ == "__main__":
    have_a_look_at_the_warehouse()
