from pyspark.sql import DataFrame
from pyspark.sql import functions as func
from src.spark import get_spark_session
from configs.config import BRONZE_DATA_DIR, SILVER_DATA_DIR

def transform_silver(df: DataFrame) -> DataFrame:
    # 1. Clean, rename, trim, and cast columns in a single projection pass
    transformed = df.select(
        func.initcap(func.trim(func.col("Youtuber"))).alias("youtuber"),
        func.initcap(func.trim(func.col("category"))).alias("category"),
        func.regexp_replace(func.col("subscribers"), ",", "").cast("long").alias("subscribers"),
        func.regexp_replace(func.col("video views"), ",", "").cast("long").alias("video_views"),
        func.regexp_replace(func.col("video count"), ",", "").cast("long").alias("video_count"),
        # Include any remaining columns from the original DataFrame if they exist
        *[func.col(c) for c in df.columns if c not in ["Youtuber", "category", "subscribers", "video views", "video count"]]
    )
    
    # 2. Deduplicate data
    transformed = transformed.drop_duplicates()
    
    # 3. Filter out invalid records (chained for clarity and performance)
    transformed = transformed.filter(
        (func.col("video_count") > 0) & 
        (func.col("video_views") > 0)
    )
    
    # 4. Add calculated metrics
    transformed = transformed.withColumns({
        "views_per_subscriber": func.when(
            func.col("subscribers") > 0, 
            func.col("video_views") / func.col("subscribers")
        ),
        "avg_views_per_video": func.col("video_views") / func.col("video_count")
    })
    
    return transformed

def write_silver(df: DataFrame, output_path: str) -> None:
    df.write.mode("overwrite").parquet(output_path)


def run_silver(): 
    spark = get_spark_session()
    bronze_path = BRONZE_DATA_DIR / "youtube_channels"
    silver_path = SILVER_DATA_DIR / "youtube_channels"
    
    bronze_df = spark.read.parquet(bronze_path)
    
    silver_df = transform_silver(bronze_df)
    
    write_silver(silver_df, silver_path)