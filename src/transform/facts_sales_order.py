import pandas as pd
from copy import deepcopy
from datetime import date, time, datetime

<<<<<<< HEAD:src/transform/facts_table.py

def facts_table(dataframe):
=======
def transform_sales_order(dataframe):
>>>>>>> main:src/transform/facts_sales_order.py
    """
    takes a sales_order OLTP dataframe

    returns a dataframe formatted for the facts_sales_orders OLAP table (minus its primary key)

    the "sales_record_id" column will need to be populated when appending to the database,
    as it is a serial primary key and it cannot be assumed beforehand
    """
    if list(dataframe.columns) != [
        "sales_order_id",
        "created_at",
        "last_updated",
        "design_id",
        "staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id",
    ]:
        raise ValueError

    facts_columns = [
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    all_rows = []
    for index in range(len(dataframe)):
        row = dataframe.loc[index]

        order_id = row.iloc[0]
        created_date = datetime.strptime(row.iloc[1], "%Y-%m-%d %H:%M:%S.%f").date()
        created_time = datetime.strptime(row.iloc[1], "%Y-%m-%d %H:%M:%S.%f").time()
        last_updated_date = datetime.strptime(
            row.iloc[2], "%Y-%m-%d %H:%M:%S.%f"
        ).date()
        last_updated_time = datetime.strptime(
            row.iloc[2], "%Y-%m-%d %H:%M:%S.%f"
        ).time()
        sales_staff_id = row.iloc[4]
        counterparty_id = row.iloc[5]
        units_sold = row.iloc[6]
        unit_price = row.iloc[7]
        currency_id = row.iloc[8]
        design_id = row.iloc[3]
        agreed_payment_date = datetime.strptime(row.iloc[10], "%Y-%m-%d").date()
        agreed_delivery_date = datetime.strptime(row.iloc[9], "%Y-%m-%d").date()
        agreed_delivery_location_id = row.iloc[11]

        new_row = [
            order_id,
            created_date,
            created_time,
            last_updated_date,
            last_updated_time,
            sales_staff_id,
            counterparty_id,
            units_sold,
            unit_price,
            currency_id,
            design_id,
            agreed_payment_date,
            agreed_delivery_date,
            agreed_delivery_location_id,
        ]

        new_series = pd.Series(data=new_row, index=facts_columns)

        all_rows.append(new_series)

    new_df = pd.DataFrame(data=all_rows, columns=facts_columns)

    return new_df
