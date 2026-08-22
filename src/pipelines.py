from src.spark import get_spark_session
from src.logger import get_logger
from src.bronze import ingest_bronze
from src.silver import transform_silver, write_silver
from src.gold import create_channel_performance, write_channel_performance
from src.timing import timer
from datetime import datetime, timezone
import sys
import warnings
from configs.config import RAW_DATA_DIR, SILVER_DATA_DIR, GOLD_DATA_DIR, BRONZE_DATA_DIR, AUDIT_DATA_DIR

warnings.filterwarnings("ignore")
logger = get_logger("pipeline") # logging instance

spark = get_spark_session()


def main():
    try:
        logger.info("=================")
        logger.info("STARTING PIPELINE")
        logger.info("=================")
        
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        
        source_path = RAW_DATA_DIR / "top_1000_msycig.csv"
        bronze_path = BRONZE_DATA_DIR / "youtube_channels"
        silver_path = SILVER_DATA_DIR / "youtube_channels"
        channel_perf_path = GOLD_DATA_DIR / "channel_performance" 
        
        try:
            with timer(name="Bronze ingestion", logger=logger):
                logger.info("Starting: Bronze Ingestion")
                logger.info(f"Reading data from source: {str(source_path)}")
                df_bronze = ingest_bronze(spark, source_path=str(source_path), output_path=str(bronze_path))
                
                logger.info(f"Bronze record count: {df_bronze.count()}")
                logger.info("Finished: Bronze Ingestion")
        except Exception as e:
            logger.error(f"Failed: Bronze Ingestion - {e}")
            sys.exit(1)
        
        try:    
            with timer("Silver transformation", logger):
                logger.info("Starting: Silver Transformation")
                df_silver = transform_silver(df_bronze) 
                
                write_silver(df_silver, str(silver_path))
                
                logger.info(f"Silver record count: {df_silver.count()}")
                logger.info("Finished: Silver Transformation")
        except Exception as e:
            logger.error(f"Failed: Silver Transformation - {e}")
            sys.exit(1)
            
        try:   
            with timer("Gold Analytics", logger):
                logger.info("Starting: Gold Analytics")
                df_channel_perf = create_channel_performance(df_silver)
                
                write_channel_performance(df_channel_perf, str(channel_perf_path))
                
                logger.info(f"Gold record count: {df_channel_perf.count()}")
                logger.info("Finished: Gold Analytics")        
        except Exception as e:
            logger.error(f"Failed: Gold Analytics - {e}")
            sys.exit(1)
            
        try:
            audit_data = [{
                "run_id": run_id,
                "pipeline_name": "germany_youtube_analytics",
                "status": "SUCCESS",
                "bronze_rows": df_bronze.count(),
                "silver_rows": df_silver.count(),
                "gold_channel_rows": df_channel_perf.count(),
            }]
            
            audit_df = spark.createDataFrame(
                audit_data,
                schema=["run_id", "pipeline_name", "status", "bronze_rows", "silver_rows", "gold_channel_rows"]
            )
            audit_path = AUDIT_DATA_DIR / "pipeline_runs"
            
            (
                audit_df.write
                .mode("append")
                .csv(str(audit_path))
            )
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        
if __name__ == "__main__":
    main()