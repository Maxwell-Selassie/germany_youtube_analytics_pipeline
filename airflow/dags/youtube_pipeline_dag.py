from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id = "germany_youtube_analytics_pipeline",
    start_date = datetime(2026, 9, 26),
    schedule = None,
    catchup = False,
    tags = ["youtube", "data-engineering", "spark", "airflow"]
)
def youtube_pipeline():
    
    @task
    def bronze():
        from src.bronze import run_bronze
        
        print("Starting bronze...")
        run_bronze
        print("Bronze completed successfully")
        
    @task
    def silver():
        from src.silver import run_silver
        
        print("Starting silver...")
        run_silver
        print("Silver completed successfully")
        
    @task
    def gold():
        from src.gold import run_gold
        
        print("Starting Gold...")
        run_gold
        print("Gold completed successfully")
        
    bronze_task = bronze()
    silver_task = silver()
    gold_task = gold()
    
    bronze_task >> silver_task >> gold_task
    
youtube_pipeline()
