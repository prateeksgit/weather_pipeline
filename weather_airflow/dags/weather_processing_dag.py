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
        # Building Bridge Table
        build_bridge_table = BigQueryExecuteQueryOperator(
            task_id="build_bridge_table",
            sql='''
                INSERT INTO `weather-data-pipeline-487815.weather_warehouse.bridge_observation_weather`
                (observation_id, weather_id)
                SELECT 
                    f.observation_id,
                    r.weather_id
                FROM `weather-data-pipeline-487815.weather_staging.current_weather` r
                JOIN `weather-data-pipeline-487815.weather_warehouse.dim_city` c 
                    ON c.city_name = r.city_name AND c.country = r.country
                JOIN `weather-data-pipeline-487815.weather_warehouse.fact_weather_observation` f
                    ON f.city_id = c.city_id 
                    AND f.observation_time = r.observation_time
                WHERE DATE(r.ingestion_timestamp) = CURRENT_DATE()
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
        )
        #Creating Mart
        #daily mart
        build_daily_mart=BigQueryExecuteQueryOperator(
            task_id='build_daily_mart',
            sql='''
            CREATE OR REPLACE TABLE `weather-data-pipeline-487815.weather_mart.fact_weather_daily`
            PARTITION BY date
            CLUSTER BY city_id
            AS
            select 
                city_id,
                DATE(observation_time) AS date,

                -- Temperature metrics
                AVG(temperature) AS avg_temperature,
                MIN(temp_min) AS min_temperature,
                MAX(temp_max) AS max_temperature,

                -- Environmental metrics
                AVG(pressure) AS avg_pressure,
                AVG(humidity) AS avg_humidity,
                AVG(wind_speed) AS avg_wind_speed,
                AVG(cloud_percentage) AS avg_cloud_percentage,
                AVG(visibility) AS avg_visibility,

                COUNT(*) AS observation_count

                from `weather-data-pipeline-487815.weather_warehouse.fact_weather_observation`
                group by city_id,DATE(observation_time)
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
            )
        #7 day Average
        build_7Day_mart=BigQueryExecuteQueryOperator(
            task_id='build_7day_mart',
            sql='''
                CREATE OR REPLACE TABLE `weather-data-pipeline-487815.weather_mart.mart_weather_7day` AS
                SELECT
                    city_id,
                    date,
                    AVG(avg_temperature) OVER (
                        PARTITION BY city_id
                        ORDER BY date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS rolling_avg_7day_temperature,
                    
                    AVG(avg_humidity) OVER (
                        PARTITION BY city_id
                        ORDER BY date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS rolling_avg_7day_humidity,
                    
                    AVG(avg_pressure) OVER (
                        PARTITION BY city_id
                        ORDER BY date
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    ) AS rolling_avg_7day_pressure

                FROM `weather-data-pipeline-487815.weather_mart.fact_weather_daily`
                
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
        )
        #Weather Trend
        build_trend_mart=BigQueryExecuteQueryOperator(
            task_id='build_weather_trend',
            sql='''
                CREATE OR REPLACE TABLE `weather-data-pipeline-487815.weather_mart.mart_weather_trend` AS 
                WITH base_table as(
                select city_id,date,
                avg_temperature,LAG(avg_temperature)over(partition by city_id order by date)
                as previous_day_temperature
                from `weather-data-pipeline-487815.weather_mart.fact_weather_daily`
                )
                select *,avg_temperature as average_temp,previous_day_temperature as temperature_change,
                CASE
                    WHEN previous_day_temperature is NULL THEN NULL
                    WHEN previous_day_temperature<avg_temperature then 'INCREASING'
                    WHEN previous_day_temperature>avg_temperature then 'DECREASING'
                    ELSE 'STABLE'
                END as temperature_trend
                from base_table
            ''',
            use_legacy_sql=False,
            gcp_conn_id='google_cloud_default'
        )
        # Dimension tables can run in parallel
        [build_dim_city, build_dim_weather] >> build_fact_observation

        # Fact depends on dimensions
        build_fact_observation >> build_bridge_table

        # Bridge depends on fact
        build_bridge_table >> build_daily_mart

        # Daily mart depends on bridge
        build_daily_mart >> [build_7Day_mart, build_trend_mart]  # Both 7-day and trend depend on daily mart
