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
        # Task 1: Building dimension tables
        build_dim_city = BigQueryExecuteQueryOperator(
            task_id='build_dim_city',
            sql='''
                MERGE `weather-data-pipeline-487815.weather_warehouse.dim_city` T
                USING (
                    SELECT DISTINCT
                        city_name,
                        country,
                        latitude,
                        longitude,
                        timezone
                    FROM `weather-data-pipeline-487815.weather_staging.current_weather`
                    WHERE DATE(ingestion_timestamp) = CURRENT_DATE() 
                    AND city_name IS NOT NULL   
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
        build_dim_weather = BigQueryExecuteQueryOperator(
            task_id='build_dim_weather',
            sql='''
                MERGE `weather-data-pipeline-487815.weather_warehouse.dim_weather_condition` T
                USING (
                    SELECT DISTINCT
                        weather_id,
                        weather_main AS main,
                        weather_description AS description,
                        weather_icon AS icon
                    FROM `weather-data-pipeline-487815.weather_staging.current_weather`
                ) S
                ON T.weather_id = S.weather_id
                WHEN NOT MATCHED THEN
                INSERT (weather_id, main, description, icon)
                VALUES (S.weather_id, S.main, S.description, S.icon)
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
        )
        # Task 2: Building facts tables
        build_fact_observation=BigQueryExecuteQueryOperator(
            task_id="build_fact_observation",
            sql='''
                MERGE `weather-data-pipeline-487815.weather_warehouse.fact_weather_observation` T
                USING (
                    SELECT
                        c.city_id,
                        r.observation_time,
                        r.temperature,
                        r.feels_like,
                        r.temp_min,
                        r.temp_max,
                        r.pressure,
                        r.sea_level,
                        r.grnd_level,
                        r.humidity,
                        r.wind_speed,
                        r.wind_degree,
                        r.visibility,
                        r.cloud_percentage
                    FROM `weather-data-pipeline-487815.weather_staging.current_weather` r
                    JOIN `weather-data-pipeline-487815.weather_warehouse.dim_city` c
                        ON c.city_name = r.city_name AND c.country = r.country
                    WHERE DATE(r.ingestion_timestamp) = CURRENT_DATE()
                ) S
                ON T.city_id = S.city_id AND T.observation_time = S.observation_time
                WHEN NOT MATCHED THEN
                    INSERT (city_id, observation_time, temperature, feels_like, temp_min, temp_max,
                            pressure, sea_level, grnd_level, humidity, wind_speed, wind_degree,
                            visibility, cloud_percentage)
                    VALUES (S.city_id, S.observation_time, S.temperature, S.feels_like, S.temp_min, S.temp_max,
                            S.pressure, S.sea_level, S.grnd_level, S.humidity, S.wind_speed, S.wind_degree,
                            S.visibility, S.cloud_percentage)
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
            )