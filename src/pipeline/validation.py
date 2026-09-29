from pyspark.sql.functions import col


class ValidationError(Exception):
    pass


def validate_menu_data(df):
    errors = []
    warnings = []

    row_count = df.count()

    # 1. Satır sayısı
    if not 50 <= row_count <= 10_000:
        errors.append(f"Beklenmeyen satır sayısı: {row_count}")

    # 2. Zorunlu sütunlar
    required_columns = [
        "MENU_ITEM_ID",
        "MENU_ITEM_NAME",
        "TRUCK_BRAND_NAME",
        "COST_OF_GOODS_USD",
        "SALE_PRICE_USD",
    ]

    missing_columns = [
        name for name in required_columns
        if name not in df.columns
    ]

    if missing_columns:
        errors.append(f"Eksik sütunlar: {missing_columns}")
        raise ValidationError("; ".join(errors))

    # 3. NULL kontrolü
    key_columns = [
        "MENU_ITEM_ID",
        "MENU_ITEM_NAME",
        "COST_OF_GOODS_USD",
        "SALE_PRICE_USD",
    ]

    for name in key_columns:
        null_count = df.filter(col(name).isNull()).count()
        null_ratio = null_count / row_count if row_count else 0

        if null_ratio > 0.05:
            errors.append(f"{name}: %{null_ratio * 100:.1f} NULL")
        elif null_count:
            warnings.append(f"{name}: {null_count} NULL değer")

    # 4. Tekrarlanan ürünler
    unique_items = df.select("MENU_ITEM_ID").distinct().count()

    if unique_items < row_count:
        warnings.append(
            f"{row_count - unique_items} tekrarlanan ürün kaydı"
        )

    # 5. Negatif fiyatlar
    negative_prices = df.filter(
        (col("COST_OF_GOODS_USD") < 0)
        | (col("SALE_PRICE_USD") < 0)
    ).count()

    if negative_prices:
        errors.append(f"{negative_prices} negatif fiyatlı kayıt")

    if errors:
        raise ValidationError("; ".join(errors))

    for warning in warnings:
        print(f"UYARI: {warning}")

    print(f"Validation başarılı: {row_count} kayıt")
    return df