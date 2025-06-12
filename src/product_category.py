"""
Одна функция: выдаёт пары «product_name – category_name».
Продукты без категорий тоже включаем (category_name = null).
"""

from pyspark.sql import DataFrame, functions as F


def get_product_category_pairs(
    products: DataFrame,
    categories: DataFrame,
    product_categories: DataFrame,
) -> DataFrame:
    """
    Args:
        products:          id, name
        categories:        id, name
        product_categories: product_id, category_id

    Returns:
        DataFrame(product_name STRING, category_name STRING | null)
    """
    return (
        products.alias("p")
        .join(  # left join сохраним все продукты
            product_categories.alias("pc"),
            F.col("p.id") == F.col("pc.product_id"),
            "left",
        )
        .join(
            categories.alias("c"),
            F.col("c.id") == F.col("pc.category_id"),
            "left",
        )
        .select(
            F.col("p.name").alias("product_name"),
            F.col("c.name").alias("category_name"),
        )
        .distinct()
    )
