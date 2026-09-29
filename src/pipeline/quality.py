from pyspark.sql.functions import col


def check_data_quality(df):
    checks_passed = 0
    total_checks = 4

    row_count = df.count()

    # 1. Output boş olmamalı
    if row_count > 0:
        checks_passed += 1
        print(f"✓ Output: {row_count} kayıt")
    else:
        print("✗ Output boş!")

    # 2. Marka + menü türü kombinasyonları benzersiz olmalı
    distinct_keys = (
        df.select("TRUCK_BRAND_NAME", "MENU_TYPE")
        .distinct()
        .count()
    )

    if distinct_keys == row_count:
        checks_passed += 1
        print("✓ Tekrarlanan marka/menü türü yok")
    else:
        print(f"✗ {row_count - distinct_keys} tekrarlanan kombinasyon")

    # 3. Ortalama kâr marjı negatif olmamalı
    negative_margins = df.filter(
        col("AVG_MARGIN_PCT") < 0
    ).count()

    if negative_margins == 0:
        checks_passed += 1
        print("✓ Negatif ortalama kâr marjı yok")
    else:
        print(f"✗ {negative_margins} negatif marjlı kayıt")

    # 4. Marka adı NULL olmamalı
    null_brands = df.filter(
        col("TRUCK_BRAND_NAME").isNull()
    ).count()

    if null_brands == 0:
        checks_passed += 1
        print("✓ Eksik marka adı yok")
    else:
        print(f"✗ {null_brands} eksik marka adı")

    quality_score = checks_passed / total_checks

    print(f"Data Quality Score: {quality_score:.0%}")

    return quality_score