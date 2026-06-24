CREATE TABLE dim_product_details (
    details_id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    region TEXT NOT NULL,
    created_on TIMESTAMP DEFAULT NOW()
);


CREATE TABLE dim_dates (
    date_id DATE PRIMARY KEY,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL
);


CREATE TABLE fact_production (
    production_id SERIAL PRIMARY KEY,

    details_id INT NOT NULL,
    date_id DATE NOT NULL,

    units_produced INT NOT NULL,
    defective_units INT NOT NULL,

    revenue NUMERIC(12,2) NOT NULL,
    cost NUMERIC(12,2) NOT NULL,

    analyst_score INT,

    created_on TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY(details_id)
        REFERENCES dim_product_details(details_id),

    FOREIGN KEY(date_id)
        REFERENCES dim_dates(date_id)
);
