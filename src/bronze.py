from pyspark.sql import DataFrame, SparkSession

def ingest_bronze(
    spark: SparkSession,
    source_path: str,
    output_path: str
) -> DataFrame:
    
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(source_path))
    )
    
    if df.isEmpty():
        raise ValueError("Bronze ingestion failed: source is empty")
    
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    
    
    return df