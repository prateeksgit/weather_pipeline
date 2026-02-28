**Weather Data Warehouse**

Designed and implemented a cloud-based ELT pipeline using BigQuery and Airflow that ingests weather API data daily, 
transforms it into a dimensional warehouse model, and produces analytical tables for reporting.

- An automated cloud data pipeline.
- Weather data extracted daily from an external API.(Openweather)
- Loads raw data into cloud warehouse. 
- Transforms it into clean analytical tables.
- Stores the data in dimensional model (fact+dimension)

The project can adresses stuffs like:

- Historical weather trends.
- Average monthly temperature
- Rainfall patterns 
- Climate comparison between cities.
- Data for dashboard insights.

Stack's used: 
- Data Source: Weather API OpenWeather.
Coud Data Warehouse: 
-Google BigQuery used since it is:
-Serverless
-SQL based
-Free tier available

Prgramming layer
- PYTHON for:
-API extraction
-Data Lodaing
-Logic building

Orchestration layer: 
-Apache airflow . 

Containerization: 
-Docker

Images: 

<img width="200" height="200" alt="Screenshot 2026-02-28 at 14 08 28" src="https://github.com/user-attachments/assets/f284a06f-801d-4e85-bb35-bbd95d182418" />
<img width="200" height="200" alt="Screenshot 2026-02-28 at 14 08 47" src="https://github.com/user-attachments/assets/bede3b7c-c3eb-4b7b-bd3b-1e8eb017c551" />
<img width="200" height="200" alt="Screenshot 2026-02-28 at 14 09 06" src="https://github.com/user-attachments/assets/30e75697-9c78-4960-82a8-5566abe9acdb" />
<img width="200" height="200" alt="Screenshot 2026-02-28 at 14 12 43" src="https://github.com/user-attachments/assets/caf30a20-7519-47fa-af47-d48be1d19368" />






