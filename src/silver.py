from pyspark.sql import DataFrame
from pyspark.sql import functions as func

def transform_silver(df: DataFrame) -> DataFrame:
    
    transformed = df 
    
    # rename columns
    transformed = transformed.withColumnRenamed("video views", "video_views")
    transformed = transformed.withColumnRenamed("video count", "video_count")
    transformed = transformed.withColumnRenamed("Youtuber", "youtuber")
    
    # trim whitespaces in string records
    col_trim = ["youtuber", "category"]
    for col in col_trim:
        transformed = transformed.withColumn(
            col,
            func.initcap(func.trim(col))
        )
        
    # deduplicate data
    transformed = transformed.drop_duplicates()
    
    # convert the type of certain string columns to long
    col_int = ["subscribers", "video_views", "video_count"]
    for col_name in col_int:
        transformed = transformed.withColumn(
            col_name,
            func.regexp_replace(func.col(col_name), ",", "").cast("long")
        )
        
    # remove records that do not meet range validation checks
    transformed = transformed.filter((transformed["video_count"] > 0) & (transformed["video_views"] > 0))
    
    # aggregate data
    # views per subscriber
    transformed = transformed.withColumn(
        "views_per_subscriber",
        func.when(
            func.col("subscribers") > 0,
            func.col("video_views") / func.col("subscribers")
        )
    )
    
    # avg_views_per_video
    transformed = transformed.withColumn(
        "avg_views_per_video",
        func.when(
            func.col("video_count") > 0,
            func.col("video_views") / func.col("video_count")
        )
    )
    
    return transformed
    
def write_silver(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )