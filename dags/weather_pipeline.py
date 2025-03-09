from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import csv

# Define constants
#API_URL = 'api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=7977a408e7d6807aef421073e6fe11b6'
API_URL = "https://api.openweathermap.org/data/2.5/weather?q=Stockholm&appid=7977a408e7d6807aef421073e6fe11b6"
DB_CONN = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432",
}

# Extract: Fetch weather data
def fetch_weather():
    response = requests.get(API_URL)
    data = response.json()
    with open('/opt/airflow/dags/weather_raw.json', 'w') as f:
        f.write(str(data))

# Load: Save data into PostgreSQL
def save_to_db():
    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(50),
                    temperature FLOAT,
                    description VARCHAR(100),
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """)
            with open('/opt/airflow/dags/weather_raw.json', 'r') as f:
                data = eval(f.read())
                cur.execute("""
                    INSERT INTO weather (city, temperature, description)
                    VALUES (%s, %s, %s)
                """, (data['name'], data['main']['temp'], data['weather'][0]['description']))
            conn.commit()

# Transform: Convert temp to Celsius and save to CSV
def transform_data():
    with psycopg2.connect(**DB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT city, temperature, description FROM weather ORDER BY timestamp DESC LIMIT 1")
            row = cur.fetchone()
            if row:
                city, temp, desc = row
                temp_celsius = temp - 273.15  # Convert Kelvin to Celsius
                with open('/opt/airflow/dags/weather_data.csv', 'w') as f:
                    writer = csv.writer(f)
                    writer.writerow(["City", "Temperature (C)", "Description"])
                    writer.writerow([city, temp_celsius, desc])

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_pipeline',
    default_args=default_args,
    description='A simple weather data pipeline',
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

task_fetch = PythonOperator(task_id='fetch_weather', python_callable=fetch_weather, dag=dag)
task_save_db = PythonOperator(task_id='save_to_db', python_callable=save_to_db, dag=dag)
task_transform = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)

# Define task dependencies
task_fetch >> task_save_db >> task_transform
