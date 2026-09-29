def write_output(spark, df, schema_name):
    table_name = (
        f"SNOWFLAKE_LEARNING_DB."
        f"{schema_name}.MENU_BRAND_SUMMARY"
    )

    df.write.mode("overwrite").saveAsTable(table_name)

    written_count = spark.read.table(table_name).count()

    if written_count != df.count():
        raise RuntimeError(
            f"Yazma doğrulaması başarısız: "
            f"Beklenen {df.count()}, bulunan {written_count}"
        )

    print(f"Snowflake tablosu: {table_name}")
    print(f"Kaydedilen kayıt sayısı: {written_count}")

    return written_count