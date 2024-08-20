-- \c <database name> 
    -- does this work with remote databases???
    -- do we need credentials built into this somehow? (env variables obviously)

-- create tables:

-- MVP:

DROP TABLE IF EXISTS dim_counterparty;
CREATE TABLE dim_counterparty (
    counterparty_id INTEGER PRIMARY KEY,
    counterparty_legal_name VARCHAR NOT NULL,
    counterparty_legal_address_line_1 VARCHAR NOT NULL,
    counterparty_legal_address_line2 VARCHAR,
    counterparty_legal_district VARCHAR,
    counterparty_legal_city VARCHAR NOT NULL,
    counterparty_legal_postal_code VARCHAR NOT NULL,
    counterparty_legal_country VARCHAR NOT NULL,
    counterparty_legal_phone_number VARCHAR NOT NULL,
);

DROP TABLE IF EXISTS dim_currency;
CREATE TABLE dim_currency (
    currency_id INTEGER PRIMARY KEY,
    currency_code VARCHAR NOT NULL,
    currency_name VARCHAR NOT NULL,
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR NOT NULL, -- do we need to specify length with VARCHAR?
    month_name VARCHAR NOT NULL, -- ditto
    quarter INTEGER NOT NULL,
);

DROP TABLE IF EXISTS dim_design;
CREATE TABLE dim_design (
    design_id INTEGER PRIMARY KEY,
    design_name VARCHAR NOT NULL,
    file_location VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL,
);

DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY
    address_line_1 VARCHAR NOT NULL
    address_line_2 VARCHAR
    district VARCHAR
    city VARCHAR NOT NULL
    postal_code VARCHAR NOT NULL
    country VARCHAR NOT NULL
    phone VARCHAR NOT NULL
);

DROP TABLE IF EXISTS dim_staff;
CREATE TABLE dim_staff (
    staff_id INTEGER PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    department_name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    email_address VARCHAR NOT NULL, -- this needs to be a valid email address, look up how to enforce that
);

DROP TABLE IF EXISTS fact_sales_order;
CREATE TABLE fact_sales_order (
    sales_record_id SERIAL PRIMARY KEY,
    sales_order_id INTEGER NOT NULL,
    created_date DATE NOT NULL,
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL,
    last_updated_time TIME NOT NULL,
    sales_staff_id INTEGER NOT NULL REFERENCES dim_staff.staff_id,
    counterparty_id INTEGER NOT NULL REFERENCES dim_counterparty.counterparty_id,
    units_sold INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL, -- need to double check this type
    currency_id INTEGER NOT NULL REFERENCES dim_currency.currency_id,
    design_id INTEGER NOT NULL REFERENCES dim_design.design_id,
    agreed_payment_date DATE NOT NULL,
    agreed_delivery_date DATE NOT NULL,
    agreed_delivery_location_id INTEGER NOT NULL REFERENCES dim_location.location_id,
);


-- extension:
-- fact_purchase_orders
-- fact_payment
-- dim_transaction
-- dim_payment_type