# AQI ETL Pipeline & Dashboard

## Overview
This project demonstrates a complete **ETL pipeline** for monitoring **Air Quality Index (AQI)** and related pollutants across Indian cities, using **Apache Airflow, PostgreSQL, and Power BI**. The pipeline collects, transforms, and visualizes air quality data in an automated workflow.

---

## Features
- Automated ETL pipeline using **Apache Airflow DAG**.
- Fetches real-time air quality data using the **[WAQI API](https://waqi.info/)**.
- Extracted parameters include:
  - **AQI**
  - **NO₂**
  - **CO**
  - Other relevant pollutants.
- Data transformation:
  - Categorized AQI into meaningful ranges (e.g., Good, Moderate, Unhealthy).
  - Cleaned and prepared the dataset for analytics.
- Loaded the processed data into a **PostgreSQL** database.
- Integrated with **Power BI**:
  - Cleaned the data using Power Query Editor.
  - Built interactive dashboards for visualizing AQI trends across cities.

---

## Tech Stack
- **Data Pipeline:** Apache Airflow
- **Database:** PostgreSQL
- **Data Source:** WAQI API (Real-time air quality data)
- **Visualization:** Power BI
- **Language:** Python (for ETL scripts)

---


---

## How It Works
1. **Extract:** The Airflow DAG fetches air quality data for a list of Indian cities (with latitude & longitude) using the WAQI API.
2. **Transform:** The raw data is cleaned, structured, and AQI values are categorized into standard levels.
3. **Load:** The transformed data is inserted into a PostgreSQL table.
4. **Visualize:** Power BI connects to PostgreSQL to create dashboards, enabling interactive analysis of AQI trends.

---

