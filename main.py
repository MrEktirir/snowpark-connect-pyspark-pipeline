from time import perf_counter
from src.pipeline.connection import create_spark_session

from src.pipeline.ingestion import (
    setup_ingestion,
    create_raw_table,
    load_raw_data,
)

from src.pipeline.validation import validate_menu_data
from src.pipeline.transformation import transform_menu_data
from src.pipeline.quality import check_data_quality
from src.pipeline.output import write_output

def main():
    start_time = perf_counter()
    spark = create_spark_session()

    try:
        # STEP 1 — Data Ingestion
        schema_name = setup_ingestion(spark)
        print(f"Schema hazır: {schema_name}")

        create_raw_table(spark)
        print("MENU_RAW tablosu oluşturuldu.")

        row_count = load_raw_data(spark)
        print(f"Yüklenen kayıt sayısı: {row_count}")

        # STEP 2 — Read Data with PySpark
        df_raw = spark.read.table(
            f"SNOWFLAKE_LEARNING_DB.{schema_name}.MENU_RAW"
        )

        df_raw.printSchema()
        df_raw.show(5)

        # STEP 3 — Data Validation
        df_validated = validate_menu_data(df_raw)

        # STEP 4 — Data Transformation
        df_transformed = transform_menu_data(df_validated)

        print(f"Özet kayıt sayısı: {df_transformed.count()}")

        df_transformed.select(
            "TRUCK_BRAND_NAME",
            "MENU_TYPE",
            "ITEM_COUNT",
            "AVG_PRICE_USD",
            "AVG_PROFIT_USD",
            "AVG_MARGIN_PCT",
        ).show(10, truncate=False)

        # STEP 5 — Data Quality Checks
        quality_score = check_data_quality(df_transformed)

        # STEP 6 — Write Output
        if quality_score < 0.75:
            raise RuntimeError(
                f"Data Quality Score yetersiz: {quality_score:.0%}"
            )

        written_count = write_output(
            spark,
            df_transformed,
            schema_name,
        )

        # STEP 7 — Pipeline Summary
        duration = perf_counter() - start_time

        print("\n" + "=" * 50)
        print("PIPELINE SUMMARY")
        print("=" * 50)
        print("Status: SUCCESS")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Rows In: {row_count}")
        print(f"Rows Out: {written_count}")
        print(f"Quality Score: {quality_score:.0%}")
        print("=" * 50)

    finally:
        # Release Spark session resources
        spark.stop()


if __name__ == "__main__":
    main()