# 🌍 AQI ETL Pipeline & Interactive Dashboard

## 📌 Overview
This project demonstrates a complete **ETL pipeline** for monitoring **Air Quality Index (AQI)** and related pollutants across Indian cities using **Apache Airflow, PostgreSQL, Python, and Power BI**.

The pipeline automatically **extracts, transforms, and loads air quality data** and presents it through an **interactive Power BI dashboard** for better insights into pollution trends across cities.

---

## 🚀 Key Features
✔ **Automated ETL pipeline** orchestrated using **Apache Airflow DAGs**  
✔ **Real-time air quality data ingestion** using the **WAQI API**  
✔ **Data preprocessing & cleaning** using **Pandas**  
✔ **AQI categorization** into meaningful air quality levels  
✔ **PostgreSQL database integration** for structured data storage  
✔ **Interactive Power BI dashboard** for AQI monitoring and trend analysis  

---

## 🗂 Data Preparation

Initially, a **dataset containing information about cities across different Indian states** was obtained.

### Data Processing Steps
1. Extracted **city names, states, latitude, and longitude** from the dataset.
2. Performed **data cleaning and preprocessing using Pandas**.
3. Used the cleaned **latitude and longitude coordinates** to fetch **real-time AQI data from the WAQI API**.
4. Structured and prepared the dataset for the ETL pipeline.

---

## ⚙️ Tech Stack

| Component | Technology |
|-----------|------------|
| 🧠 Data Pipeline | Apache Airflow |
| 🐍 Programming | Python |
| 🗄 Database | PostgreSQL |
| 🌐 Data Source | WAQI API |
| 📊 Visualization | Power BI |
| 🧹 Data Cleaning | Pandas |

---

## 🔄 ETL Pipeline Workflow

### 1️⃣ Extract
The Airflow DAG retrieves **air quality data for multiple Indian cities** using the **WAQI API**, based on the **latitude and longitude** obtained from the cleaned dataset.

### 2️⃣ Transform
The raw data is processed and transformed:
- Cleaned and structured using **Pandas**
- Selected important pollutants such as:
  - **AQI**
  - **NO₂**
  - **CO**
  - Other key parameters
- Categorized AQI values into standard air quality categories:

| AQI Range | Category |
|-----------|----------|
| 0 – 50 | 🟢 Good |
| 51 – 100 | 🟡 Moderate |
| 101 – 150 | 🟠 Unhealthy for Sensitive Groups |
| 151 – 200 | 🔴 Unhealthy |
| 201 – 300 | 🟣 Very Unhealthy |
| 301+ | ⚫ Hazardous |

---

## 📊 Visualization

Power BI connects to the PostgreSQL database to:

- Clean the data using **Power Query Editor**
- Build **interactive dashboards**
- Analyze **AQI trends across Indian cities**
- Monitor **pollution levels and pollutant distribution**

---

## 📈 Dashboard Insights

The Power BI dashboard enables users to:

- 📍 Monitor **AQI levels across cities**
- 📈 Analyze **pollution trends over time**
- 🌎 Compare **air quality across states**
- 🧪 Study pollutant distribution such as **NO₂ and CO**

---

## 💡 Project Highlights

- Built a **fully automated ETL workflow using Airflow DAGs**
- Integrated **real-time environmental data APIs**
- Applied **data cleaning and transformation techniques**
- Designed an **analytics-ready data warehouse**
- Developed an **interactive Power BI dashboard for insights**

---

## 🔮 Future Improvements

- Add **historical AQI trend forecasting**
- Implement **real-time streaming pipelines**
- Deploy dashboards on **Power BI Service**
- Add **more pollutants and environmental metrics**
