import pandas as pd
from copy import deepcopy
from datetime import date, time, datetime

def facts_table(dataframe):
    """
    takes a sales_order OLTP dataframe

    returns a dataframe formatted for the facts_sales_orders OLAP table (minus its primary key)

    the "sales_record_id" column will need to be populated when appending to the database, 
    as it is a serial primary key and it cannot be assumed beforehand
    """

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

        order_id = row[0]
        created_date = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f").date()
        created_time = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f").time()
        last_updated_date = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S.%f").date()
        last_updated_time = datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S.%f").time()
        sales_staff_id = row[4]
        counterparty_id = row[5]
        units_sold = row[6]
        unit_price = row[7]
        currency_id = row[8]
        design_id = row[3]
        agreed_payment_date = datetime.strptime(row[10], "%Y-%m-%d").date()
        agreed_delivery_date = datetime.strptime(row[9], "%Y-%m-%d").date()
        agreed_delivery_location_id = row[11]

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





    #     0"sales_order_id",
    #     1"created_at",
    #     2"last_updated",
    #     3"design_id",
    #     4"staff_id",
    #     5"counterparty_id",
    #     6"units_sold",
    #     7"unit_price", # ❌
    #     8"currency_id",
    #     9"agreed_delivery_date", # ❌ gotta fix this in our dim date test suite too!
    #     10"agreed_payment_date", # ❌
    #     11"agreed_delivery_location_id",



    new_df = pd.DataFrame(data=all_rows, columns=facts_columns)

    return new_df