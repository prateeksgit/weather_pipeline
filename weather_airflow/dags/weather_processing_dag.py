from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

with DAG (
    'weather_processing',
    description='DAG for dim/fact/marts',
    schedule_interval=None,
    start_date=datetime(2026,2,25),
    catchup=False,
    ) as dag:

        wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion',
        external_dag_id='weather_ingestion',
        external_task_id=None,
        timeout=3600,
        execution_delta=None,  
        check_existence=True,  
        poke_interval=30,
        )

        # Task 1: Build dimension tables
        build_dim_city = BigQueryExecuteQueryOperator(
            task_id='build_dim_city',
            sql='''
                MERGE `weather-data-pipeline-487815.weather_warehouse.dim_city` T
                USING (
                    SELECT DISTINCT
                        city AS city_name,
                        country,
                        latitude,
                        longitude,
                        timezone
                    FROM `weather-data-pipeline-487815.weather_staging.current_weather`
                    WHERE DATE(ingestion_timestamp) = CURRENT_DATE() 
                    AND city IS NOT NULL   
                ) S
                ON T.city_name = S.city_name AND T.country = S.country
                WHEN MATCHED THEN
                    UPDATE SET
                        latitude=S.latitude,
                        longitude=S.longitude,
                        timezone=S.timezone
                WHEN NOT MATCHED THEN
                INSERT (city_name, country, latitude, longitude, timezone) 
                VALUES (S.city_name, S.country, S.latitude, S.longitude, S.timezone)
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
        )
        