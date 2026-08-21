from pyspark.sql import DataFrame
# from pyspark.sql import functions as func

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
    
# def create_category_performance(df: DataFrame) -> DataFrame:
#     return (
#         df.groupBy("category")
#         .agg(
#             func.sum("video_count").alias("total_video_count_per_category"),
#         df.groupBy("category").agg(func.avg("video_views").alias("avg_views_per_category"))
#         )
#     )

def write_channel_performance(df: DataFrame, output_path: str) -> None:
    (
        df.write.mode("overwrite").parquet(output_path)
    )