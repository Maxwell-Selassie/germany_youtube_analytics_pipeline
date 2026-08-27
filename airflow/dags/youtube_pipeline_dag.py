from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="germany_youtube_analytics_pipeline",
    start_date=datetime(2026, 9, 26),
    schedule=None,
    catchup=False,
    tags=[
        "youtube",
        "data-engineering",
        "spark",
        "duckdb",
        "dbt",
        "airflow",
    ],
)
def youtube_pipeline():

    # ============================================================
    # BRONZE
    # ============================================================

    @task
    def bronze():
        from src.bronze import run_bronze

        print("Starting Bronze ingestion...")

        run_bronze()

        print("Bronze completed successfully.")

    # ============================================================
    # SILVER
    # ============================================================

    @task
    def silver():
        from src.silver import run_silver

        print("Starting Silver transformation...")

        run_silver()

        print("Silver completed successfully.")

    # ============================================================
    # LOAD SILVER INTO DUCKDB
    # ============================================================

    @task
    def load_duckdb():

        import duckdb

        silver_path = (
            "/opt/airflow/project/data/silver/youtube_channels/*.parquet"
        )

        database_path = (
            "/opt/airflow/project/"
            "germany_youtube_analytics_pipeline_dbt/"
            "dev.duckdb"
        )

        print("Loading Silver data into DuckDB...")

        con = duckdb.connect(database_path)

        con.execute(
            "CREATE SCHEMA IF NOT EXISTS raw"
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE raw.youtube_channels AS
            SELECT *
            FROM read_parquet('{silver_path}')
            """
        )

        row_count = con.execute(
            "SELECT COUNT(*) FROM raw.youtube_channels"
        ).fetchone()[0]

        con.close()

        print(
            f"Successfully loaded {row_count:,} rows "
            "into raw.youtube_channels."
        )

    # ============================================================
    # DBT BUILD
    # ============================================================

    @task
    def dbt_build():

        import subprocess

        dbt_project = (
            "/opt/airflow/project/"
            "germany_youtube_analytics_pipeline_dbt"
        )

        print("Starting dbt build...")

        result = subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                dbt_project,
            ],
            cwd=dbt_project,
            check=True,
        )

        print("dbt build completed successfully.")

    # ============================================================
    # PIPELINE DEPENDENCIES
    # ============================================================

    bronze_task = bronze()

    silver_task = silver()

    duckdb_task = load_duckdb()

    dbt_task = dbt_build()

    bronze_task >> silver_task >> duckdb_task >> dbt_task


# ================================================================
# DAG REGISTRATION
# ================================================================

youtube_pipeline()

