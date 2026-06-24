import logging
from google.cloud import bigquery


# ---------------------------------
# CONFIGURATION
# ---------------------------------

PROJECT_ID = "calm-sky-419511"
DATASET = "Nathanelt_sales"

SOURCE_TABLE = (
    f"{PROJECT_ID}.{DATASET}.production"
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------------------------
# BIGQUERY CONNECTION
# ---------------------------------

client = bigquery.Client(
    project=PROJECT_ID
)

logging.info("Connected to BigQuery")


# ---------------------------------
# CREATE TABLES
# ---------------------------------

dim_product_sql = f"""

CREATE TABLE IF NOT EXISTS
`{PROJECT_ID}.{DATASET}.dim_product_details`

(
    details_id INT64,
    product_name STRING,
    category STRING,
    region STRING,
    created_on TIMESTAMP
)

"""


dim_date_sql = f"""

CREATE TABLE IF NOT EXISTS
`{PROJECT_ID}.{DATASET}.dim_dates`

(
    date_id DATE,
    year INT64,
    month INT64,
    day INT64
)

"""


fact_sql = f"""

CREATE TABLE IF NOT EXISTS
`{PROJECT_ID}.{DATASET}.fact_production`

(
    production_id INT64,

    details_id INT64,

    date_id DATE,

    units_produced INT64,

    defective_units INT64,

    revenue FLOAT64,

    cost FLOAT64,

    analyst_score INT64,

    created_on TIMESTAMP
)

"""


# ---------------------------------
# TRANSFORM DIM PRODUCT
# ---------------------------------

insert_dim_product = f"""

INSERT INTO
`{PROJECT_ID}.{DATASET}.dim_product_details`

SELECT DISTINCT

ABS(FARM_FINGERPRINT(product_name)),

product_name,

category,

region,

CURRENT_TIMESTAMP()

FROM `{SOURCE_TABLE}`

"""


# ---------------------------------
# TRANSFORM DATE DIMENSION
# ---------------------------------

insert_dates = f"""

INSERT INTO
`{PROJECT_ID}.{DATASET}.dim_dates`

SELECT DISTINCT

production_date,

EXTRACT(YEAR FROM production_date),

EXTRACT(MONTH FROM production_date),

EXTRACT(DAY FROM production_date)


FROM `{SOURCE_TABLE}`

"""


# ---------------------------------
# TRANSFORM FACT TABLE
# ---------------------------------

insert_fact = f"""

INSERT INTO
`{PROJECT_ID}.{DATASET}.fact_production`

SELECT

ROW_NUMBER() OVER(),

p.details_id,

r.production_date,

r.units_produced,

r.defective_units,

r.revenue,

r.cost,

r.analyst_score,

CURRENT_TIMESTAMP()


FROM `{SOURCE_TABLE}` r


JOIN 
`{PROJECT_ID}.{DATASET}.dim_product_details` p


ON r.product_name = p.product_name

"""


# ---------------------------------
# QUERY RUNNER
# ---------------------------------

def run_query(sql, task_name):

    logging.info(
        f"Starting: {task_name}"
    )

    job = client.query(sql)

    job.result()


    logging.info(
        f"Completed: {task_name}"
    )


# ---------------------------------
# ETL PIPELINE FUNCTION
# ---------------------------------

def run_etl_pipeline():

    run_query(
        dim_product_sql,
        "Create Product Dimension"
    )


    run_query(
        dim_date_sql,
        "Create Date Dimension"
    )


    run_query(
        fact_sql,
        "Create Fact Table"
    )


    run_query(
        insert_dim_product,
        "Load Product Dimension"
    )


    run_query(
        insert_dates,
        "Load Date Dimension"
    )


    run_query(
        insert_fact,
        "Load Fact Table"
    )


    logging.info(
        "ETL Pipeline Finished Successfully"
    )


# ---------------------------------
# EXECUTION
# ---------------------------------

