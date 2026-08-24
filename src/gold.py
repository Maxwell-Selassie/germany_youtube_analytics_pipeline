from pyspark.sql import DataFrame
from pyspark.sql import functions as func

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