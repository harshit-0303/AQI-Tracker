from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import requests
import pandas as pd

POSTGRES_CONN_ID = "postgres_default"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# -----------------------------
# Task 1: Start Pipeline
# -----------------------------
def start_pipeline():
    print("AQI Pipeline Started")


# -----------------------------
# Task 2: Create Tables
# -----------------------------
def create_tables():

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_locations_table = """
    CREATE TABLE IF NOT EXISTS locations_aqi (
        id SERIAL PRIMARY KEY,
        state VARCHAR(100),
        city VARCHAR(100),
        latitude FLOAT,
        longitude FLOAT,
        aqi INT,
        pm2_5 FLOAT,
        pm10 FLOAT,
        co FLOAT,
        no2 FLOAT,
        category VARCHAR(50),
        timestamp TIMESTAMP
    );
    """

    postgres_hook.run(create_locations_table)

    print("Tables created successfully")


# -----------------------------
# Task 3: Extract Data
# -----------------------------
def extract_data(dag_run=None, **kwargs):

    state_list = dag_run.conf.get("state")

    if isinstance(state_list, str):
        state_list = [state_list]

    df = pd.read_csv("/usr/local/airflow/data/State_geo_list.csv")
    filtered = df[df["state"].isin(state_list)]

    API_TOKEN = "e9550dbcd41fc1c17d8cd0aa54a45f584616aa94"

    session = requests.Session()

    extracted = []

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT city, MAX(timestamp)
        FROM locations_aqi
        WHERE state IN %s
        GROUP BY city
    """, (tuple(state_list),))

    latest_data = dict(cursor.fetchall())

    cursor.close()
    conn.close()

    def fetch_city_aqi(row):

        city = row["city"]
        lat = row["latitude"]
        lon = row["longitude"]

        url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={API_TOKEN}"

        try:

            response = session.get(url, timeout=5)

            if response.status_code != 200:
                print(f"Skipping {city} due to bad response: {response.status_code}")
                return None

            data = response.json()

            if data.get("status") != "ok":
                print(f"API returned error for {city}, skipping...")
                return None

            iaqi = data["data"].get("iaqi", {})

            timestamp_raw = data["data"]["time"]["iso"]
            timestamp_dt = datetime.fromisoformat(timestamp_raw)

            if city in latest_data and timestamp_dt <= latest_data[city]:
                return None

            timestamp = timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")

            return {
                "state": row["state"],
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "aqi": data["data"].get("aqi"),
                "pm2_5": iaqi.get("pm25", {}).get("v"),
                "pm10": iaqi.get("pm10", {}).get("v"),
                "co": iaqi.get("co", {}).get("v"),
                "no2": iaqi.get("no2", {}).get("v"),
                "timestamp": timestamp
            }

        except requests.exceptions.Timeout:
            print(f"Timeout for {city}, skipping...")
            return None

        except requests.exceptions.RequestException as e:
            print(f"Request failed for {city}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=20) as executor:

        futures = [
            executor.submit(fetch_city_aqi, row)
            for row in filtered.to_dict("records")
        ]

        for future in as_completed(futures):

            result = future.result()

            if result:
                extracted.append(result)

    print(f"Extracted {len(extracted)} records")

    return extracted

# -----------------------------
# Task 4: Transform Data
# -----------------------------
def transform_data(**kwargs):

    ti = kwargs["ti"]

    extracted = ti.xcom_pull(task_ids="extract_data")

    if not extracted:
        print("No data extracted")
        return []

    def get_category(aqi):

        if aqi is None:
            return "Unknown"

        if aqi <= 50:
            return "Good"
        elif aqi <= 100:
            return "Moderate"
        elif aqi <= 150:
            return "Unhealthy for Sensitive Groups"
        elif aqi <= 200:
            return "Unhealthy"
        elif aqi <= 300:
            return "Very Unhealthy"
        else:
            return "Hazardous"

    for row in extracted:
        row["category"] = get_category(row["aqi"])

    print("Transformation complete")

    return extracted


# -----------------------------
# Task 5: Load Data
# -----------------------------
def load_data(**kwargs):

    ti = kwargs["ti"]

    transformed = ti.xcom_pull(task_ids="transform_data")

    if not transformed:
        print("No data to load")
        return

    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO locations_aqi
    (state, city, latitude, longitude, aqi, pm2_5, pm10, co, no2, category, timestamp)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    values = []

    for row in transformed:

        timestamp = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")

        values.append(
            (
                row["state"],
                row["city"],
                row["latitude"],
                row["longitude"],
                row["aqi"],
                row["pm2_5"],
                row["pm10"],
                row["co"],
                row["no2"],
                row["category"],
                timestamp
            )
        )

    cursor.executemany(insert_query, values)

    conn.commit()

    cursor.close()
    conn.close()

    print(f"Loaded {len(values)} records into locations_aqi")


# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="EndToEndPipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    default_args=default_args,
    catchup=False
) as dag:

    t0 = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline
    )

    t1 = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    t2 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    t3 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    t4 = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    t0 >> t1 >> t2 >> t3 >> t4