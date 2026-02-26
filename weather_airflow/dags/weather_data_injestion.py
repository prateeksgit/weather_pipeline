import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 


# Default args
default_args = {
    'owner': 'pratik',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_ingestion',
    default_args=default_args,
    description='Fetch weather API and load into BigQuery',
    schedule_interval='@hourly',
    start_date=datetime(2026, 2, 23),
    catchup=False,
    tags=['weather', 'pipeline'],
) as dag:

    def fetch_and_load_weather(**context):
        # Cities to process
        cities = ['Kathmandu', 'Pokhara','Jomsom']
        
        # API Key 
        API_KEY = "49dd56311e9d736aed7151c281c2d888"
        
        # Setting up credentials
        credentials_path = '/opt/airflow/keys/gcp_airflow_key.json'
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        # Creating BigQuery client
        client = bigquery.Client(
            project='weather-data-pipeline-487815',
            credentials=credentials
        )
        
        # Definining table and schema 
        table_id = "weather-data-pipeline-487815.weather_staging.current_weather"
        schema = [
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("feels_like", "FLOAT"),
            bigquery.SchemaField("humidity", "INT64"),
            bigquery.SchemaField("pressure", "INT64"),
            bigquery.SchemaField("weather_main", "STRING"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_degree", "FLOAT"),
            bigquery.SchemaField("observation_time", "TIMESTAMP"),
            bigquery.SchemaField("sunrise", "TIMESTAMP"),
            bigquery.SchemaField("sunset", "TIMESTAMP"),
            bigquery.SchemaField("timezone", "INT64"),
            bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP"),
        ]
        
        # Ensuring table exists (do this ONCE before the loop)
        try:
            client.get_table(table_id)
            print(f"Table {table_id} exists.")
        except Exception:
            print(f"Table {table_id} not found. Creating table...")
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)
            print(f"Created table {table_id}.")
        
        # LOOPING THROUGH EACH CITY
        for city in cities:
            try:
                print(f"\n ===== Processing {city} ===== ")
                
                # 1. FETCHING API for this city
                url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                print(f"API Response received for {data['name']}, {data['sys']['country']}")
                
                # 2. SAVING JSON locally
                output_dir = "/opt/airflow/dags/weather_json"
                os.makedirs(output_dir, exist_ok=True)
                date_str = context['ds_nodash']
                hour = context['execution_date'].hour
                output_file = os.path.join(output_dir, f"weather_{city}_{date_str}_{hour:02d}.json")
                with open(output_file, "w") as f:
                    json.dump(data, f, indent=4)
                print(f"Saved JSON to {output_file}")

                query = f"""
                    MERGE `{table_id}` T
                    USING (
                    SELECT
                        @city AS city,
                        @country AS country,
                        @latitude AS latitude,
                        @longitude AS longitude,
                        @temperature AS temperature,
                        @feels_like AS feels_like,
                        @humidity AS humidity,
                        @pressure AS pressure,
                        @weather_main AS weather_main,
                        @weather_description AS weather_description,
                        @wind_speed AS wind_speed,
                        @wind_degree AS wind_degree,
                        TIMESTAMP_SECONDS(@observation_time) AS observation_time,
                        TIMESTAMP_SECONDS(@sunrise) AS sunrise,
                        TIMESTAMP_SECONDS(@sunset) AS sunset,
                        @timezone AS timezone,
                        CURRENT_TIMESTAMP() as ingestion_timestamp
                    ) S
                    ON T.city = S.city AND T.observation_time = S.observation_time
                    WHEN MATCHED THEN
                    UPDATE SET
                        temperature = S.temperature,
                        feels_like = S.feels_like,
                        humidity = S.humidity,
                        pressure = S.pressure,
                        weather_main = S.weather_main,
                        weather_description = S.weather_description,
                        wind_speed = S.wind_speed,
                        wind_degree = S.wind_degree,
                        sunrise = S.sunrise,
                        sunset = S.sunset,
                        timezone=S.timezone,
                        ingestion_timestamp = S.ingestion_timestamp
                    WHEN NOT MATCHED THEN
                    INSERT (city, country, latitude, longitude, temperature, feels_like,
                            humidity, pressure, weather_main, weather_description,
                            wind_speed, wind_degree, observation_time, sunrise, sunset,timezone,ingestion_timestamp)
                    VALUES (S.city, S.country, S.latitude, S.longitude, S.temperature, S.feels_like,
                            S.humidity, S.pressure, S.weather_main, S.weather_description,
                            S.wind_speed, S.wind_degree, S.observation_time, S.sunrise, S.sunset,S.timezone, S.ingestion_timestamp)
                    """

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("city", "STRING", data["name"]),
                        bigquery.ScalarQueryParameter("country", "STRING", data["sys"]["country"]),
                        bigquery.ScalarQueryParameter("latitude", "FLOAT64", data["coord"]["lat"]),
                        bigquery.ScalarQueryParameter("longitude", "FLOAT64", data["coord"]["lon"]),
                        bigquery.ScalarQueryParameter("temperature", "FLOAT64", data["main"]["temp"]),
                        bigquery.ScalarQueryParameter("feels_like", "FLOAT64", data["main"]["feels_like"]),
                        bigquery.ScalarQueryParameter("humidity", "INT64", int(data["main"]["humidity"])),
                        bigquery.ScalarQueryParameter("pressure", "INT64", int(data["main"]["pressure"])),
                        bigquery.ScalarQueryParameter("weather_main", "STRING", data["weather"][0]["main"]),
                        bigquery.ScalarQueryParameter("weather_description", "STRING", data["weather"][0]["description"]),
                        bigquery.ScalarQueryParameter("wind_speed", "FLOAT64", data["wind"]["speed"]),
                        bigquery.ScalarQueryParameter("wind_degree", "INT64", data["wind"]["deg"]),
                        bigquery.ScalarQueryParameter("observation_time", "INT64", data["dt"]),
                        bigquery.ScalarQueryParameter("sunrise", "INT64", data["sys"]["sunrise"]),
                        bigquery.ScalarQueryParameter("sunset", "INT64", data["sys"]["sunset"]),
                        bigquery.ScalarQueryParameter("timezone", "INT64", data["timezone"]),
                    ]
                )
                
                # 4. INSERTING into BigQuery
                job = client.query(query, job_config=job_config)
                job.result()
                print(f"MERGE completed for {city} - rows affected: {job.num_dml_affected_rows}")
                    
            except Exception as e:
                print(f"Failed to process {city}: {str(e)}")
                # Continuing with next city even if one fails
                continue
        
        print("\n All cities processed!")

    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_processing',
        trigger_dag_id='weather_processing', 
        wait_for_completion=False, 
        conf={"source": "ingestion_triggered"}, 
    )
    
    weather_task = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=fetch_and_load_weather,
        provide_context=True
    )
    weather_task >> trigger_processing