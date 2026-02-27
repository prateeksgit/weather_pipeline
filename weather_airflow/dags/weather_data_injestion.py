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
            # City identifiers
            bigquery.SchemaField("city_name", "STRING"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("timezone", "INT64"),
            
            # Temperature metrics
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("feels_like", "FLOAT"),
            bigquery.SchemaField("temp_min", "FLOAT"),
            bigquery.SchemaField("temp_max", "FLOAT"),
            
            # Pressure metrics
            bigquery.SchemaField("pressure", "INT64"),
            bigquery.SchemaField("sea_level", "INT64"),
            bigquery.SchemaField("grnd_level", "INT64"),
            
            # Other weather metrics
            bigquery.SchemaField("humidity", "INT64"),
            bigquery.SchemaField("visibility", "INT64"),
            bigquery.SchemaField("cloud_percentage", "INT64"),
            
            # Wind metrics
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_degree", "INT64"),
            
            # Weather condition
            bigquery.SchemaField("weather_id", "INT64"),
            bigquery.SchemaField("weather_main", "STRING"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("weather_icon", "STRING"),
            
            # Time fields
            bigquery.SchemaField("observation_time", "TIMESTAMP"),
            bigquery.SchemaField("sunrise", "TIMESTAMP"),
            bigquery.SchemaField("sunset", "TIMESTAMP"),
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
                            @city_name AS city_name,
                            @country AS country,
                            @latitude AS latitude,
                            @longitude AS longitude,
                            @timezone AS timezone,
                            @temperature AS temperature,
                            @feels_like AS feels_like,
                            @temp_min AS temp_min,
                            @temp_max AS temp_max,
                            @pressure AS pressure,
                            @sea_level AS sea_level,
                            @grnd_level AS grnd_level,
                            @humidity AS humidity,
                            @visibility AS visibility,
                            @cloud_percentage AS cloud_percentage,
                            @wind_speed AS wind_speed,
                            @wind_degree AS wind_degree,
                            @weather_id AS weather_id,
                            @weather_main AS weather_main,
                            @weather_description AS weather_description,
                            @weather_icon AS weather_icon,
                            TIMESTAMP_SECONDS(@observation_time) AS observation_time,
                            TIMESTAMP_SECONDS(@sunrise) AS sunrise,
                            TIMESTAMP_SECONDS(@sunset) AS sunset,
                            CURRENT_TIMESTAMP() AS ingestion_timestamp
                    ) S
                    ON T.city_name = S.city_name AND T.observation_time = S.observation_time
                    WHEN MATCHED THEN
                        UPDATE SET
                            temperature = S.temperature,
                            feels_like = S.feels_like,
                            temp_min = S.temp_min,
                            temp_max = S.temp_max,
                            pressure = S.pressure,
                            sea_level = S.sea_level,
                            grnd_level = S.grnd_level,
                            humidity = S.humidity,
                            visibility = S.visibility,
                            cloud_percentage = S.cloud_percentage,
                            wind_speed = S.wind_speed,
                            wind_degree = S.wind_degree,
                            weather_id = S.weather_id,
                            weather_main = S.weather_main,
                            weather_description = S.weather_description,
                            weather_icon = S.weather_icon,
                            sunrise = S.sunrise,
                            sunset = S.sunset,
                            timezone = S.timezone,
                            ingestion_timestamp = S.ingestion_timestamp
                    WHEN NOT MATCHED THEN
                        INSERT (
                            city_name, country, latitude, longitude, timezone,
                            temperature, feels_like, temp_min, temp_max,
                            pressure, sea_level, grnd_level, humidity, visibility, cloud_percentage,
                            wind_speed, wind_degree,
                            weather_id, weather_main, weather_description, weather_icon,
                            observation_time, sunrise, sunset, ingestion_timestamp
                        )
                        VALUES (
                            S.city_name, S.country, S.latitude, S.longitude, S.timezone,
                            S.temperature, S.feels_like, S.temp_min, S.temp_max,
                            S.pressure, S.sea_level, S.grnd_level, S.humidity, S.visibility, S.cloud_percentage,
                            S.wind_speed, S.wind_degree,
                            S.weather_id, S.weather_main, S.weather_description, S.weather_icon,
                            S.observation_time, S.sunrise, S.sunset, S.ingestion_timestamp
                        )
                """

                job_config = bigquery.QueryJobConfig(
                    query_parameters = [
                    # City identifiers
                    bigquery.ScalarQueryParameter("city_name", "STRING", data["name"]),
                    bigquery.ScalarQueryParameter("country", "STRING", data["sys"]["country"]),
                    bigquery.ScalarQueryParameter("latitude", "FLOAT64", data["coord"]["lat"]),
                    bigquery.ScalarQueryParameter("longitude", "FLOAT64", data["coord"]["lon"]),
                    bigquery.ScalarQueryParameter("timezone", "INT64", data["timezone"]),
                    
                    # Temperature metrics
                    bigquery.ScalarQueryParameter("temperature", "FLOAT64", data["main"]["temp"]),
                    bigquery.ScalarQueryParameter("feels_like", "FLOAT64", data["main"]["feels_like"]),
                    bigquery.ScalarQueryParameter("temp_min", "FLOAT64", data["main"]["temp_min"]),
                    bigquery.ScalarQueryParameter("temp_max", "FLOAT64", data["main"]["temp_max"]),
                    
                    # Pressure metrics
                    bigquery.ScalarQueryParameter("pressure", "INT64", int(data["main"]["pressure"])),
                    bigquery.ScalarQueryParameter("sea_level", "INT64", int(data["main"].get("sea_level", 0))),
                    bigquery.ScalarQueryParameter("grnd_level", "INT64", int(data["main"].get("grnd_level", 0))),
                    
                    # Other weather metrics
                    bigquery.ScalarQueryParameter("humidity", "INT64", int(data["main"]["humidity"])),
                    bigquery.ScalarQueryParameter("visibility", "INT64", int(data.get("visibility", 0))),
                    bigquery.ScalarQueryParameter("cloud_percentage", "INT64", int(data["clouds"]["all"])),
                    
                    # Wind metrics
                    bigquery.ScalarQueryParameter("wind_speed", "FLOAT64", data["wind"]["speed"]),
                    bigquery.ScalarQueryParameter("wind_degree", "INT64", int(data["wind"]["deg"])),
                    
                    # Weather condition (first element)
                    bigquery.ScalarQueryParameter("weather_id", "INT64", int(data["weather"][0]["id"])),
                    bigquery.ScalarQueryParameter("weather_main", "STRING", data["weather"][0]["main"]),
                    bigquery.ScalarQueryParameter("weather_description", "STRING", data["weather"][0]["description"]),
                    bigquery.ScalarQueryParameter("weather_icon", "STRING", data["weather"][0]["icon"]),
                    
                    # Time fields
                    bigquery.ScalarQueryParameter("observation_time", "INT64", data["dt"]),
                    bigquery.ScalarQueryParameter("sunrise", "INT64", data["sys"]["sunrise"]),
                    bigquery.ScalarQueryParameter("sunset", "INT64", data["sys"]["sunset"]),
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