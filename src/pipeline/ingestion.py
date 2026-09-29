from snowflake.snowpark_connect.snowflake_session import SnowflakeSession


def setup_ingestion(spark):
    sf = SnowflakeSession(spark)

    current_user = sf.sql("SELECT CURRENT_USER()").collect()[0][0]
    schema_name = f"{current_user}_INTRO_TO_SNOWPARK_CONNECT"

    sf.sql(
        f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
    ).collect()

    sf.sql(
        f"USE SCHEMA {schema_name}"
    ).collect()

    sf.sql("""
        CREATE OR REPLACE STAGE blob_stage
        URL = 's3://sfquickstarts/tastybytes/'
        FILE_FORMAT = (TYPE = CSV)
    """).collect()

    return schema_name

def create_raw_table(spark):
    sf = SnowflakeSession(spark)

    sf.sql("""
        CREATE OR REPLACE TABLE MENU_RAW (
            MENU_ID NUMBER(19,0),
            MENU_TYPE_ID NUMBER(38,0),
            MENU_TYPE VARCHAR,
            TRUCK_BRAND_NAME VARCHAR,
            MENU_ITEM_ID NUMBER(38,0),
            MENU_ITEM_NAME VARCHAR,
            ITEM_CATEGORY VARCHAR,
            ITEM_SUBCATEGORY VARCHAR,
            COST_OF_GOODS_USD NUMBER(38,4),
            SALE_PRICE_USD NUMBER(38,4),
            MENU_ITEM_HEALTH_METRICS_OBJ VARIANT
        )
    """).collect()

def load_raw_data(spark):
    sf = SnowflakeSession(spark)

    sf.sql("""
        COPY INTO MENU_RAW
        FROM @blob_stage/raw_pos/menu/
    """).collect()

    return sf.sql("SELECT COUNT(*) FROM MENU_RAW").collect()[0][0]