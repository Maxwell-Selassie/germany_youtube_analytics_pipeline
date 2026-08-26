from pyspark.sql import DataFrame, SparkSession
from src.spark import get_spark_session
from configs.config import RAW_DATA_DIR, BRONZE_DATA_DIR

def ingest_bronze(
    spark: SparkSession,
    source_path: str,
) -> DataFrame:
    
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(source_path))
    )
    
    if df.isEmpty():
        raise ValueError("Bronze ingestion failed: source is empty")
    
    
    return df

def write_bronze(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    

def run_bronze():
    spark = get_spark_session()
    
    source_path = RAW_DATA_DIR / "top_1000_msycig.csv"
    bronze_path = BRONZE_DATA_DIR / "youtube_channels"
    
    bronze_df = ingest_bronze(
        spark=spark,
        source_path=source_path
    )
    
    write_bronze(bronze_df, bronze_path)