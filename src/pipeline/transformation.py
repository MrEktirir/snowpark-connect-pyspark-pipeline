from uuid import uuid4

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    round,
    avg,
    sum,
    min,
    max,
    count,
    lit,
    current_timestamp,
)


def transform_menu_data(df):
    # 5A — Data Cleaning
    df_clean = (
        df
        .withColumn("TRUCK_BRAND_NAME", trim(upper(col("TRUCK_BRAND_NAME"))))
        .withColumn("ITEM_CATEGORY", trim(upper(col("ITEM_CATEGORY"))))
        .withColumn("MENU_TYPE", trim(upper(col("MENU_TYPE"))))
        .filter(col("COST_OF_GOODS_USD").isNotNull())
        .filter(col("SALE_PRICE_USD").isNotNull())
    )

    # 5B — Profit Calculations
    df_profit = (
        df_clean
        .withColumn(
            "PROFIT_USD",
            round(col("SALE_PRICE_USD") - col("COST_OF_GOODS_USD"), 2),
        )
        .withColumn(
            "PROFIT_MARGIN_PCT",
            round(
                (col("SALE_PRICE_USD") - col("COST_OF_GOODS_USD"))
                / col("SALE_PRICE_USD") * 100,
                2,
            ),
        )
    )

    # 5C — Categorization
    df_categorized = (
        df_profit
        .withColumn(
            "PROFIT_TIER",
            when(col("PROFIT_MARGIN_PCT") >= 70, "Premium")
            .when(col("PROFIT_MARGIN_PCT") >= 50, "High")
            .when(col("PROFIT_MARGIN_PCT") >= 30, "Medium")
            .otherwise("Low"),
        )
        .withColumn(
            "PRICE_TIER",
            when(col("SALE_PRICE_USD") >= 10, "Premium")
            .when(col("SALE_PRICE_USD") >= 5, "Mid-Range")
            .otherwise("Value"),
        )
    )

    # 5D — Brand Aggregation
    df_summary = (
        df_categorized
        .groupBy("TRUCK_BRAND_NAME", "MENU_TYPE")
        .agg(
            count("*").alias("ITEM_COUNT"),
            round(avg("COST_OF_GOODS_USD"), 2).alias("AVG_COST_USD"),
            round(avg("SALE_PRICE_USD"), 2).alias("AVG_PRICE_USD"),
            round(avg("PROFIT_USD"), 2).alias("AVG_PROFIT_USD"),
            round(avg("PROFIT_MARGIN_PCT"), 2).alias("AVG_MARGIN_PCT"),
            round(min("PROFIT_USD"), 2).alias("MIN_PROFIT_USD"),
            round(max("PROFIT_USD"), 2).alias("MAX_PROFIT_USD"),
            round(sum("PROFIT_USD"), 2).alias("TOTAL_POTENTIAL_PROFIT_USD"),
        )
        .orderBy(col("AVG_MARGIN_PCT").desc())
    )

    return (
        df_summary
        .withColumn("PIPELINE_RUN_ID", lit(uuid4().hex[:8]))
        .withColumn("PROCESSED_AT", current_timestamp())
    )