import pytest
from chispa.dataframe_comparer import assert_df_equality


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .appName("product_category_test")
        .getOrCreate()
    )


def test_pairs(spark):
    # фиктивные данные
    products = spark.createDataFrame(
        [(1, "milk"), (2, "bread"), (3, "coffee")],
        ["id", "name"],
    )
    categories = spark.createDataFrame(
        [(10, "dairy"), (11, "bakery")],
        ["id", "name"],
    )
    links = spark.createDataFrame(
        [(1, 10), (2, 11)],  # milk→dairy, bread→bakery
        ["product_id", "category_id"],
    )

    from src.product_category import get_product_category_pairs

    res = get_product_category_pairs(products, categories, links)

    expected = spark.createDataFrame(
        [
            ("milk", "dairy"),
            ("bread", "bakery"),
            ("coffee", None),  # без категории
        ],
        ["product_name", "category_name"],
    )

    assert_df_equality(res, expected, ignore_nullable=True)
