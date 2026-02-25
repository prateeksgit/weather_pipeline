import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

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
    'weather_data_pipeline',
    default_args=default_args,
    description='Fetch weather API and load into BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 23),
    catchup=False,
    tags=['weather', 'pipeline'],
) as dag:

    def fetch_and_load_weather(**context):
        # Cities to process
        cities = ['Kathmandu', 'Pokhara']
        
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
            bigquery.SchemaField("humidity", "FLOAT"),
            bigquery.SchemaField("pressure", "FLOAT"),
            bigquery.SchemaField("weather_main", "STRING"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_degree", "FLOAT"),
            bigquery.SchemaField("observation_time", "TIMESTAMP"),
            bigquery.SchemaField("sunrise", "TIMESTAMP"),
            bigquery.SchemaField("sunset", "TIMESTAMP"),
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
                execution_date = context['ds']
                output_file = os.path.join(output_dir, f"weather_{city}_{execution_date}.json")
                with open(output_file, "w") as f:
                    json.dump(data, f, indent=4)
                print(f"Saved JSON to {output_file}")
                
                # 3. PREPARING ROW for BigQuery
                row = [{
                    "city": data["name"],
                    "country": data["sys"]["country"],
                    "latitude": data["coord"]["lat"],
                    "longitude": data["coord"]["lon"],
                    "temperature": data["main"]["temp"],
                    "feels_like": data["main"]["feels_like"],
                    "humidity": data["main"]["humidity"],
                    "pressure": data["main"]["pressure"],
                    "weather_main": data["weather"][0]["main"],
                    "weather_description": data["weather"][0]["description"],
                    "wind_speed": data["wind"]["speed"],
                    "wind_degree": data["wind"]["deg"],
                    "observation_time": datetime.utcfromtimestamp(data["dt"]).isoformat(),
                    "sunrise": datetime.utcfromtimestamp(data["sys"]["sunrise"]).isoformat(),
                    "sunset": datetime.utcfromtimestamp(data["sys"]["sunset"]).isoformat()
                }]
                
                # 4. INSERTING into BigQuery
                errors = client.insert_rows_json(table_id, row)
                if errors:
                    print(f"Errors for {city}: {errors}")
                else:
                    print(f"Successfully loaded {city} into BigQuery")
                    
            except Exception as e:
                print(f"Failed to process {city}: {str(e)}")
                # Continuing with next city even if one fails
                continue
        
        print("\n All cities processed!")

    weather_task = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=fetch_and_load_weather,
        provide_context=True
    )