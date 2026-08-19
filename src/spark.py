from pyspark.sql import SparkSession

def get_spark_session() -> SparkSession:
    """Create and return spark session for this project"""
    spark = (
        SparkSession.builder
        .appName("GermanYoutubeAnalyticsPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    
    return spark