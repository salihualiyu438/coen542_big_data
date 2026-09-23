# COEN542 Energy Analytics Platform

A Big Data Analytics platform for processing and analyzing household energy consumption data using Apache Kafka, MongoDB, Apache Spark, and Streamlit.

## Overview

The system implements an end-to-end Big Data analytics pipeline:

```text
Energy Consumption Data
        |
        v
Apache Kafka
        |
        v
MongoDB
        |
        v
Apache Spark
        |
        +-----------------------------+
        |                             |
        v                             v
Analytics Results              Scalability Analysis
        |                             |
        +-------------+---------------+
                      |
                      v
               Streamlit Dashboard
```

The platform supports data ingestion, distributed processing, descriptive analytics, anomaly detection, scalability evaluation, and interactive visualization.

## Key Features

- Energy consumption data ingestion using Apache Kafka
- Persistent storage using MongoDB
- Distributed data processing and analytics using Apache Spark
- Descriptive energy consumption analysis
- Hourly, daily, monthly, and weekday consumption analysis
- Household-level consumption analysis
- Peak consumption identification
- Statistical summary of energy consumption
- IQR-based anomaly detection
- Scalability experiment using multiple dataset sizes
- Interactive Streamlit dashboard
- Export of analytics results to CSV files
- Visualization of analytical and scalability results

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python 3.11 |
| Data Ingestion | Apache Kafka |
| Database | MongoDB |
| Big Data Processing | Apache Spark / PySpark |
| MongoDB-Spark Integration | MongoDB Spark Connector 10.4.0 |
| Dashboard | Streamlit |
| Data Analysis | Pandas, PySpark |
| Visualization | Matplotlib |
| Environment | Windows |

## Project Structure

```text
big_data_analytics/
├── dashboard/
│   └── app.py
├── spark/
│   ├── spark_processing.py
│   └── scalability_experiment.py
├── kafka/
│   ├── kafka_producer.py
│   └── kafka_to_mongodb.py
├── results/
│   ├── hourly_consumption/
│   ├── daily_consumption/
│   ├── monthly_consumption/
│   ├── household_consumption/
│   ├── weekday_consumption/
│   ├── overall_statistics/
│   ├── peak_hour/
│   ├── peak_day/
│   ├── peak_month/
│   ├── anomaly_detection/
│   ├── anomaly_summary/
│   ├── cleaned_sample/
│   └── scalability_experiment/
├── requirements.txt
└── README.md
```

## Data Pipeline

### 1. Data Ingestion

Energy consumption records are published to the Kafka topic:

```text
energy-consumption
```

The Kafka producer reads the source energy dataset and publishes records to Kafka.

### 2. Data Storage

The Kafka consumer receives the records and stores them in MongoDB.

Database:

```text
energy_analytics
```

Collection:

```text
energy_consumption
```

### 3. Spark Processing

Apache Spark reads the stored energy consumption records from MongoDB through the MongoDB Spark Connector.

The Spark processing stage performs:

- Data cleaning
- Missing-value analysis
- Temporal feature extraction
- Aggregation
- Statistical analysis
- Peak consumption analysis
- Household analysis
- Anomaly detection

### 4. Analytics

The platform generates:

- Hourly consumption
- Daily consumption
- Monthly consumption
- Weekday consumption
- Household consumption
- Overall statistics
- Peak hour
- Peak day
- Peak month
- Anomaly detection results
- Anomaly statistical summary

### 5. Dashboard

The Streamlit application reads the generated analytics results and presents them through an interactive dashboard.

## Big Data Analytics

The system performs meaningful analytics on the energy consumption dataset.

### Descriptive Analytics

The platform calculates:

- Total energy consumption
- Average half-hour consumption
- Minimum consumption
- Maximum consumption
- Standard deviation
- Consumption by hour
- Consumption by day
- Consumption by month
- Consumption by weekday
- Consumption by household

### Peak Consumption Analysis

The system identifies periods with the highest aggregated energy consumption:

- Peak hour
- Peak day
- Peak month

### Anomaly Detection

An IQR-based statistical method is implemented to identify unusually high or low energy consumption records.

The anomaly analysis provides:

- Number of anomalous records
- Anomaly rate
- Lower threshold
- Upper threshold
- Statistical summary of detected anomalies

### Scalability Experiment

The system includes a scalability experiment that evaluates Spark processing performance using:

- 25% of the dataset
- 50% of the dataset
- 100% of the dataset

Each dataset size is processed multiple times, with median execution time and throughput recorded.

Results are saved as:

```text
results/scalability_experiment/scalability_results.csv
results/scalability_experiment/scalability_execution_time.png
results/scalability_experiment/scalability_throughput.png
```

## Dataset

The system processes household electricity consumption records containing fields such as:

```text
LCLid
stdorToU
DateTime
energy_kwh
```

The processed MongoDB collection contains approximately 887,542 records.

The Spark processing stage identifies missing energy values and produces a cleaned dataset for analytics.

## Installation

### Prerequisites

Install the following software:

- Python 3.11
- Java JDK 17
- Apache Kafka
- MongoDB
- Apache Spark 4.x

### Clone the Repository

```bash
git clone <repository-url>
cd app
```

### Create a Virtual Environment

```bash
python -m venv coen542_env
```

Activate it on Windows:

```powershell
coen542_env\Scripts\activate
```

### Install Python Dependencies

```powershell
pip install -r requirements.txt
```

## Configuration

Ensure Java 17 is configured before running Spark.

Example:

```powershell
$env:JAVA_HOME="C:\Program Files\Java\jdk-17"
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
```

Verify the Java installation:

```powershell
java -version
```

## Running the Pipeline

### Start MongoDB

Ensure the MongoDB server is running locally.

The application uses:

```text
mongodb://127.0.0.1:27017
```

### Start Kafka

Start the Kafka broker and create the required topic:

```text
energy-consumption
```

### Run the Kafka Producer

```powershell
python kafka/kafka_producer.py
```

### Run the Kafka Consumer

```powershell
python kafka/kafka_to_mongodb.py
```

The consumer stores incoming records in MongoDB.

### Run Spark Analytics

From the project root:

```powershell
python spark/spark_processing.py
```

The generated analytics are stored in:

```text
results/
```

### Run the Scalability Experiment

```powershell
python spark/scalability_experiment.py
```

The experiment generates CSV and visualization files in:

```text
results/scalability_experiment/
```

### Run the Dashboard

```powershell
streamlit run dashboard/app.py
```

The dashboard will be available at:

```text
http://localhost:8501
```

## Generated Results

The Spark processing stage produces result datasets for:

```text
hourly_consumption
daily_consumption
monthly_consumption
household_consumption
weekday_consumption
overall_statistics
peak_hour
peak_day
peak_month
anomaly_detection
anomaly_summary
cleaned_sample
```

These outputs are consumed by the Streamlit dashboard.

## System Workflow

```text
                    +----------------------+
                    | Energy Dataset       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Apache Kafka         |
                    | energy-consumption   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Kafka Consumer       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | MongoDB              |
                    | energy_analytics     |
                    | energy_consumption   |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    | Apache Spark         |
                    | Data Processing      |
                    +----------+-----------+
                               |
              +----------------+----------------+
              |                                 |
              v                                 v
    +----------------------+          +----------------------+
    | Analytics Results    |          | Scalability Testing  |
    +----------+-----------+          +----------+-----------+
               |                                 |
               +----------------+----------------+
                                |
                                v
                     +----------------------+
                     | Streamlit Dashboard  |
                     +----------------------+
```

## Project Objectives

The project demonstrates the application of Big Data technologies to energy consumption analytics through:

1. Data ingestion using Kafka
2. Persistent storage using MongoDB
3. Large-scale processing using Apache Spark
4. Descriptive and statistical analytics
5. Anomaly detection
6. Scalability measurement
7. Interactive visualization

## Academic Context

This project was developed for the COEN542 Big Data Analytics project.

The implementation demonstrates an end-to-end analytics workflow combining data ingestion, storage, distributed processing, analytical computation, scalability evaluation, and visualization.

## License

This project is intended for academic and educational purposes.
