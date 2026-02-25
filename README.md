**Weather Data Warehouse**

Designed and implemented a cloud-based ELT pipeline using BigQuery and Airflow that ingests weather API data daily, 
transforms it into a dimensional warehouse model, and produces analytical tables for reporting.

-An automated cloud data pipeline.
- Weather data extracted daily from an external API.
- Loads raw data into cloud warehouse. 
- Transforms it into clean analytical tables.
-Stores the data in dimensional model (fact+dimension)

The project can adresses stuffs like:

-Historical weather trends.
-Average monthly temperature
-Rainfall patterns 
-Climate comparison between cities.
-Data for dashboard insights.

Stack's used: 
-Data Source: Weather API OpenWeather.
Coud Data Warehouse: 
Google BigQuery used since it is:
-Serverless
-Industry Standard
-SQL based
-Free tier available

Prgramming layer- PYTHON
-API extraction
-Data Lodaing
-Logic building

Orchestration layer: 
-Apache airflow . 
Containerization: 
-Docker
