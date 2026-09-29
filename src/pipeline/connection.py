from pathlib import Path

import yaml
from snowflake.snowpark_connect import init_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "snowflake.yaml"


def create_spark_session():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    connection_parameters = config["snowflake"]

    return init_spark_session(
        connection_parameters=connection_parameters,
        app_name="snowpark-connect-pyspark-pipeline",
    )