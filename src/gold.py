from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from src.spark import get_spark_session
from configs.config import SILVER_DATA_DIR, GOLD_DATA_DIR

def create_channel_performance(df: DataFrame) -> DataFrame:
    return df.select(
        "rank", 
        "youtuber",
        "subscribers",
        "video_views",
        "video_count",
        "category",
        "started"
    )
    
def create_category_performance(df: DataFrame) -> DataFrame:
    # Aggregated both total video count and average views in a single pass
    return (
        df.groupBy("category")
        .agg(
            func.sum("video_count").alias("total_video_count_per_category"),
            func.avg("video_views").alias("avg_views_per_category")
        )
    )

def write_channel_performance(df: DataFrame, output_path: str) -> None:
    (
        df.write.mode("overwrite").parquet(output_path)
    )

def write_category_performance(df: DataFrame, output_path: str) -> None:
    # Writes the aggregated gold-layer data out as a parquet file
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    
    
def run_gold():
    spark = get_spark_session()
    
    silver_path = SILVER_DATA_DIR / "youtube_channels"
    
    channel_perf_path = GOLD_DATA_DIR / "channel_performance" 
    category_perf_path = GOLD_DATA_DIR / "category_performance" 
    
    silver_df = spark.read.parquet(silver_path)
    
    channel_perf_df = create_channel_performance(silver_df)
    category_perf_df = create_category_performance(silver_df)
    
    write_channel_performance(channel_perf_df, channel_perf_path)
    write_category_performance(category_perf_df, category_perf_path)