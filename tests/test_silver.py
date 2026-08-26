import pytest
from pyspark.sql import SparkSession
from src.silver import transform_silver


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_silver")
        .getOrCreate()
    )
    yield spark
    spark.stop()


COLUMNS = [
    "rank",
    "Youtuber",
    "subscribers",
    "video views",
    "video count",
    "category",
    "started",
]


def test_transform_silver_row_count(spark):
    data = [
        (1, "Channel A ", "25,000,000", "10,000,000", "1,230", "Music", 2018),
        (2, "Channel B", "45,000,000", "14,000,000", "1,239", "Gaming", 2020),
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df)
    assert result.count() == 2


def test_string_trim_and_initcap(spark):
    # leading/trailing whitespace and inconsistent casing should be normalized
    data = [
        (1, "  channel a", "25,000,000", "10,000,000", "1,230", "mUSIC", 2018),
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df).collect()[0]
    assert result["youtuber"] == "Channel A"
    assert result["category"] == "Music"


def test_numeric_columns_cast_and_comma_stripped(spark):
    data = [
        (1, "Channel A", "25,000,000", "10,000,000", "1,230", "Music", 2018),
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df).collect()[0]
    assert result["subscribers"] == 25_000_000
    assert result["video_views"] == 10_000_000
    assert result["video_count"] == 1230
    # confirm actual long type, not leftover string
    assert dict(transform_silver(df).dtypes)["subscribers"] == "bigint"


def test_filter_excludes_zero_or_negative_video_count(spark):
    data = [
        (1, "Channel A", "25,000,000", "10,000,000", "0", "Music", 2018),      # video_count = 0 -> excluded
        (2, "Channel B", "45,000,000", "14,000,000", "1,239", "Gaming", 2020),  # kept
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df)
    assert result.count() == 1
    assert result.collect()[0]["youtuber"] == "Channel B"


def test_filter_excludes_zero_video_views(spark):
    data = [
        (1, "Channel A", "25,000,000", "0", "1,230", "Music", 2018),           # video_views = 0 -> excluded
        (2, "Channel B", "45,000,000", "14,000,000", "1,239", "Gaming", 2020),  # kept
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df)
    assert result.count() == 1
    assert result.collect()[0]["youtuber"] == "Channel B"


def test_deduplication(spark):
    data = [
        (1, "Channel A", "25,000,000", "10,000,000", "1,230", "Music", 2018),
        (1, "Channel A", "25,000,000", "10,000,000", "1,230", "Music", 2018),  # exact duplicate
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df)
    assert result.count() == 1


def test_derived_columns_values(spark):
    data = [
        (1, "Channel A", "25,000,000", "10,000,000", "1,000", "Music", 2018),
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df).collect()[0]
    assert result["views_per_subscriber"] == pytest.approx(10_000_000 / 25_000_000)
    assert result["avg_views_per_video"] == pytest.approx(10_000_000 / 1_000)


def test_views_per_subscriber_null_when_zero_subscribers(spark):
    # subscribers == 0 is not filtered out, so views_per_subscriber should be null,
    # not a divide-by-zero error or a 0 value
    data = [
        (1, "Channel A", "0", "10,000,000", "1,000", "Music", 2018),
    ]
    df = spark.createDataFrame(data, COLUMNS)
    result = transform_silver(df).collect()[0]
    assert result["subscribers"] == 0
    assert result["views_per_subscriber"] is None
    # video_count > 0 so this should still compute normally
    assert result["avg_views_per_video"] == pytest.approx(10_000_000 / 1_000)