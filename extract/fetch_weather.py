import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

API_KEY= os.getenv("OPENWEATHER_API_KEY")
CITY="Kathmandu"
URL= f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
# Make the request
response = requests.get(URL)

def fetch_weather_data():
    response=requests.get(URL)
    response.raise_for_status()
    return response.json()

def save_raw(data):
    timestamp= datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename= f"data/weather_raw_{timestamp}.jsonl"

    with open(filename,"w")as f:
        json.dump(data,f)
        f.write("\n")

if __name__=="__main__":
    weather_data=fetch_weather_data()
    save_raw(weather_data)
    print("Weather Data has been fetched and saved successfully")