-- \c <database name> 
    -- does this work with remote databases???
    -- do we need credentials built into this somehow? (env variables obviously)

-- create tables:
-- MVP:

DROP TABLE IF EXISTS dim_counterparty;
CREATE TABLE dim_counterparty (
    counterparty_id INTEGER PRIMARY KEY,
    counterparty_legal_name VARCHAR(20) NOT NULL,
    counterparty_legal_address_line_1 VARCHAR(50) NOT NULL,
    counterparty_legal_address_line2 VARCHAR(50),
    counterparty_legal_district VARCHAR(20),
    counterparty_legal_city VARCHAR(20) NOT NULL,
    counterparty_legal_postal_code VARCHAR(10) NOT NULL,
    counterparty_legal_country VARCHAR(20) NOT NULL,
    counterparty_legal_phone_number VARCHAR(20) NOT NULL,
);

DROP TABLE IF EXISTS dim_currency;
CREATE TABLE dim_currency (
    currency_id INTEGER PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL,
    currency_name VARCHAR(20) NOT NULL,
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(9) NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    quarter INTEGER NOT NULL,
);

DROP TABLE IF EXISTS dim_design;
CREATE TABLE dim_design (
    design_id INTEGER PRIMARY KEY,
    design_name VARCHAR(50) NOT NULL,
    file_location VARCHAR(50) NOT NULL,
    file_name VARCHAR(50) NOT NULL,
);

DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location (
    location_id INTEGER PRIMARY KEY
    address_line_1 VARCHAR(50) NOT NULL
    address_line_2 VARCHAR(50)
    district VARCHAR(20)
    city VARCHAR(20) NOT NULL
    postal_code VARCHAR(10) NOT NULL
    country VARCHAR(20) NOT NULL
    phone VARCHAR(20) NOT NULL
);

DROP TABLE IF EXISTS dim_staff;
CREATE TABLE dim_staff (
    staff_id INTEGER PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20) NOT NULL,
    department_name VARCHAR(20) NOT NULL,
    location VARCHAR(20) NOT NULL,
    email_address VARCHAR(50) NOT NULL CHECK(email_address LIKE "%@%.%") 
        -- I think this should work? to be tested once we have access
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
    unit_price NUMERIC(10, 2) NOT NULL,
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