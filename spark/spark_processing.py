import os
import shutil

# ============================================================
# COEN542 ENERGY ANALYTICS
# SPARK PROCESSING + ANALYTICS
# ============================================================
#
# Pipeline:
# MongoDB -> PySpark -> Cleaning -> Feature Engineering
#         -> Descriptive Analytics
#         -> Trend/Pattern Analysis
#         -> Anomaly Detection
#         -> CSV Results -> Streamlit Dashboard
#
# ============================================================


# ============================================================
# FORCE CORRECT WINDOWS ENVIRONMENT
# ============================================================

# IMPORTANT:
# Use the Java/Python installations that were successfully
# tested on this machine.

JAVA_HOME = r"C:\Program Files\Java\jdk-17"
PYTHON_EXE = r"C:\PROGRA~1\PYTHON~1\python.exe"
HADOOP_HOME = r"C:\hadoop"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

# Put Java and Hadoop binaries first in PATH.
os.environ["PATH"] = (
    JAVA_HOME + r"\bin;"
    + HADOOP_HOME + r"\bin;"
    + os.environ.get("PATH", "")
)


# ============================================================
# IMPORTS
# ============================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    coalesce,
    to_timestamp,
    to_date,
    year,
    month,
    dayofmonth,
    hour,
    date_format,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    stddev,
    count,
    lit,
    when,
    abs as spark_abs
)


# ============================================================
# PROJECT CONFIGURATION
# ============================================================

MONGO_URI = "mongodb://127.0.0.1:27017"
DATABASE = "energy_analytics"
COLLECTION = "energy_consumption"

# Project root:
# C:\Users\User\Documents\coen542 project\app
BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

RESULTS_DIR = os.path.join(
    BASE_DIR,
    "results"
)


# ============================================================
# SPARK SESSION
# ============================================================

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("COEN542 Energy Analytics")
        .master("local[*]")

        # MongoDB Spark Connector
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0"
        )

        # MongoDB configuration
        .config(
            "spark.mongodb.read.connection.uri",
            MONGO_URI
        )

        # Windows/local Spark settings
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        .config(
            "spark.driver.memory",
            "4g"
        )

        .config(
            "spark.ui.enabled",
            "true"
        )

        .getOrCreate()
    )

    return spark


# ============================================================
# READ DATA FROM MONGODB
# ============================================================

def read_from_mongodb(spark):

    df = (
        spark.read
        .format("mongodb")

        .option(
            "spark.mongodb.read.connection.uri",
            MONGO_URI
        )

        .option(
            "spark.mongodb.read.database",
            DATABASE
        )

        .option(
            "spark.mongodb.read.collection",
            COLLECTION
        )

        .load()
    )

    return df


# ============================================================
# DATA CLEANING + FEATURE ENGINEERING
# ============================================================

def prepare_data(df):

    # Different records may contain slightly different
    # timestamp precision. Try both formats.
    timestamp_value = coalesce(
        to_timestamp(
            col("DateTime"),
            "yyyy-MM-dd HH:mm:ss.SSSSSSS"
        ),
        to_timestamp(
            col("DateTime"),
            "yyyy-MM-dd HH:mm:ss"
        ),
        to_timestamp(col("DateTime"))
    )

    cleaned = (
        df

        # Convert DateTime into Spark timestamp
        .withColumn(
            "DateTime",
            timestamp_value
        )

        # Remove records where energy consumption is missing
        .filter(
            col("energy_kwh").isNotNull()
        )

        # Remove records with invalid timestamps
        .filter(
            col("DateTime").isNotNull()
        )

        # Remove MongoDB internal ID
        .drop("_id")

        # Date features
        .withColumn(
            "date",
            to_date("DateTime")
        )

        .withColumn(
            "year",
            year("DateTime")
        )

        .withColumn(
            "month",
            month("DateTime")
        )

        .withColumn(
            "day",
            dayofmonth("DateTime")
        )

        .withColumn(
            "hour",
            hour("DateTime")
        )

        .withColumn(
            "day_of_week",
            date_format(
                "DateTime",
                "EEEE"
            )
        )
    )

    return cleaned


# ============================================================
# SAVE SPARK DATAFRAME AS CSV
# ============================================================

def write_csv(df, folder_name):

    path = os.path.join(
        RESULTS_DIR,
        folder_name
    )

    # Remove previous result first.
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except Exception:
            pass

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )

    print(
        f"Saved: {path}"
    )


# ============================================================
# ANOMALY DETECTION
# ============================================================
#
# Method:
# Interquartile Range (IQR)
#
# Q1 = 25th percentile
# Q3 = 75th percentile
# IQR = Q3 - Q1
#
# Lower Bound = Q1 - 1.5 * IQR
# Upper Bound = Q3 + 1.5 * IQR
#
# Any energy reading outside these bounds is considered
# a statistical anomaly.
#
# This is appropriate for identifying unusually high or
# unusually low household energy consumption.
# ============================================================

def perform_anomaly_detection(cleaned):

    print("\n========================================")
    print("ANOMALY DETECTION")
    print("========================================")

    # Calculate quartiles using Spark.
    quantiles = cleaned.approxQuantile(
        "energy_kwh",
        [0.25, 0.75],
        0.001
    )

    q1 = float(quantiles[0])
    q3 = float(quantiles[1])

    iqr = q3 - q1

    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)

    print(
        f"Q1: {q1:.6f} kWh"
    )

    print(
        f"Q3: {q3:.6f} kWh"
    )

    print(
        f"IQR: {iqr:.6f} kWh"
    )

    print(
        f"Lower bound: {lower_bound:.6f} kWh"
    )

    print(
        f"Upper bound: {upper_bound:.6f} kWh"
    )

    # Identify anomalies.
    anomalies = (
        cleaned

        .withColumn(
            "anomaly",
            when(
                (col("energy_kwh") < lit(lower_bound))
                |
                (col("energy_kwh") > lit(upper_bound)),
                lit(True)
            )
            .otherwise(lit(False))
        )

        .withColumn(
            "anomaly_type",
            when(
                col("energy_kwh") > lit(upper_bound),
                lit("High Consumption")
            )
            .when(
                col("energy_kwh") < lit(lower_bound),
                lit("Low Consumption")
            )
            .otherwise(
                lit("Normal")
            )
        )
    )

    anomaly_records = (
        anomalies
        .filter(
            col("anomaly") == True
        )
        .orderBy(
            col("energy_kwh").desc()
        )
    )

    anomaly_count = anomaly_records.count()

    total_count = cleaned.count()

    anomaly_percentage = (
        (anomaly_count / total_count) * 100
        if total_count > 0
        else 0
    )

    print(
        f"Total cleaned records: {total_count:,}"
    )

    print(
        f"Anomalous records: {anomaly_count:,}"
    )

    print(
        f"Anomaly percentage: {anomaly_percentage:.4f}%"
    )

    print("\n--- TOP ANOMALOUS READINGS ---")

    anomaly_records.select(
        "DateTime",
        "LCLid",
        "energy_kwh",
        "hour",
        "day_of_week",
        "anomaly_type"
    ).show(
        10,
        truncate=False
    )

    # --------------------------------------------------------
    # ANOMALY SUMMARY
    # --------------------------------------------------------

    high_count = (
        anomaly_records
        .filter(
            col("anomaly_type") == "High Consumption"
        )
        .count()
    )

    low_count = (
        anomaly_records
        .filter(
            col("anomaly_type") == "Low Consumption"
        )
        .count()
    )

    summary = (
        cleaned
        .select(
            lit(total_count).alias(
                "total_records"
            ),
            lit(q1).alias(
                "q1_kwh"
            ),
            lit(q3).alias(
                "q3_kwh"
            ),
            lit(iqr).alias(
                "iqr_kwh"
            ),
            lit(lower_bound).alias(
                "lower_bound_kwh"
            ),
            lit(upper_bound).alias(
                "upper_bound_kwh"
            ),
            lit(anomaly_count).alias(
                "anomaly_count"
            ),
            lit(high_count).alias(
                "high_consumption_anomalies"
            ),
            lit(low_count).alias(
                "low_consumption_anomalies"
            ),
            lit(anomaly_percentage).alias(
                "anomaly_percentage"
            )
        )
        .limit(1)
    )

    return anomaly_records, summary


# ============================================================
# MAIN
# ============================================================

def main():

    # Create results directory.
    os.makedirs(
        RESULTS_DIR,
        exist_ok=True
    )

    print("\n========================================")
    print("COEN542 ENERGY ANALYTICS - SPARK")
    print("========================================")

    print(
        f"Python executable: "
        f"{os.environ['PYSPARK_PYTHON']}"
    )

    print(
        f"Java: {os.environ['JAVA_HOME']}"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        # ====================================================
        # 1. READ FROM MONGODB
        # ====================================================

        df = read_from_mongodb(
            spark
        )

        raw_count = df.count()

        missing_count = (
            df
            .filter(
                col("energy_kwh").isNull()
            )
            .count()
        )

        print(
            f"MongoDB records read: "
            f"{raw_count:,}"
        )

        print(
            f"Missing energy records: "
            f"{missing_count:,}"
        )

        # ====================================================
        # 2. CLEAN + TRANSFORM
        # ====================================================

        cleaned = prepare_data(
            df
        )

        cleaned_count = cleaned.count()

        print(
            f"Records after cleaning: "
            f"{cleaned_count:,}"
        )

        # ====================================================
        # 3. HOURLY CONSUMPTION
        # ====================================================

        hourly = (
            cleaned
            .groupBy("hour")
            .agg(
                spark_sum(
                    "energy_kwh"
                ).alias(
                    "total_energy_kwh"
                )
            )
            .orderBy("hour")
        )

        # ====================================================
        # 4. DAILY CONSUMPTION
        # ====================================================

        daily = (
            cleaned
            .groupBy("date")
            .agg(
                spark_sum(
                    "energy_kwh"
                ).alias(
                    "total_energy_kwh"
                )
            )
            .orderBy("date")
        )

        # ====================================================
        # 5. MONTHLY CONSUMPTION
        # ====================================================

        monthly = (
            cleaned
            .groupBy(
                "year",
                "month"
            )
            .agg(
                spark_sum(
                    "energy_kwh"
                ).alias(
                    "total_energy_kwh"
                )
            )
            .orderBy(
                "year",
                "month"
            )
        )

        # ====================================================
        # 6. HOUSEHOLD ANALYSIS
        # ====================================================

        household = (
            cleaned
            .groupBy("LCLid")
            .agg(
                spark_sum(
                    "energy_kwh"
                ).alias(
                    "total_energy_kwh"
                ),

                avg(
                    "energy_kwh"
                ).alias(
                    "average_half_hour_kwh"
                ),

                spark_min(
                    "energy_kwh"
                ).alias(
                    "minimum_half_hour_kwh"
                ),

                spark_max(
                    "energy_kwh"
                ).alias(
                    "maximum_half_hour_kwh"
                ),

                stddev(
                    "energy_kwh"
                ).alias(
                    "stddev_half_hour_kwh"
                ),

                count(
                    "energy_kwh"
                ).alias(
                    "readings"
                )
            )
            .orderBy(
                col(
                    "total_energy_kwh"
                ).desc()
            )
        )

        # ====================================================
        # 7. WEEKDAY ANALYSIS
        # ====================================================

        weekday = (
            cleaned
            .groupBy(
                "day_of_week"
            )
            .agg(
                spark_sum(
                    "energy_kwh"
                ).alias(
                    "total_energy_kwh"
                ),
                avg(
                    "energy_kwh"
                ).alias(
                    "average_energy_kwh"
                ),
                count(
                    "energy_kwh"
                ).alias(
                    "readings"
                )
            )
            .orderBy(
                "day_of_week"
            )
        )

        # ====================================================
        # 8. OVERALL STATISTICS
        # ====================================================

        statistics = (
            cleaned
            .select(
                spark_sum(
                    "energy_kwh"
                ).alias(
                    "total_energy_kwh"
                ),

                avg(
                    "energy_kwh"
                ).alias(
                    "average_half_hour_kwh"
                ),

                spark_min(
                    "energy_kwh"
                ).alias(
                    "minimum_half_hour_kwh"
                ),

                spark_max(
                    "energy_kwh"
                ).alias(
                    "maximum_half_hour_kwh"
                ),

                stddev(
                    "energy_kwh"
                ).alias(
                    "stddev_half_hour_kwh"
                ),

                count(
                    "energy_kwh"
                ).alias(
                    "valid_readings"
                )
            )
        )

        # ====================================================
        # 9. PEAK HOUR
        # ====================================================

        peak_hour = (
            hourly
            .orderBy(
                col(
                    "total_energy_kwh"
                ).desc()
            )
            .limit(1)
        )

        # ====================================================
        # 10. PEAK DAY
        # ====================================================

        peak_day = (
            daily
            .orderBy(
                col(
                    "total_energy_kwh"
                ).desc()
            )
            .limit(1)
        )

        # ====================================================
        # 11. PEAK MONTH
        # ====================================================

        peak_month = (
            monthly
            .orderBy(
                col(
                    "total_energy_kwh"
                ).desc()
            )
            .limit(1)
        )

        # ====================================================
        # 12. ANOMALY DETECTION
        # ====================================================

        anomaly_records, anomaly_summary = (
            perform_anomaly_detection(
                cleaned
            )
        )

        # ====================================================
        # DISPLAY ANALYTICAL RESULTS
        # ====================================================

        print("\n========================================")
        print("ANALYTICAL RESULTS")
        print("========================================")

        print("\n--- PEAK HOUR ---")
        peak_hour.show(
            truncate=False
        )

        print("\n--- PEAK DAY ---")
        peak_day.show(
            truncate=False
        )

        print("\n--- PEAK MONTH ---")
        peak_month.show(
            truncate=False
        )

        print("\n--- OVERALL STATISTICS ---")
        statistics.show(
            truncate=False
        )

        print("\n--- ANOMALY SUMMARY ---")
        anomaly_summary.show(
            truncate=False
        )

        # ====================================================
        # SAVE RESULTS
        # ====================================================

        print("\n========================================")
        print("SAVING ANALYTICS RESULTS")
        print("========================================")

        write_csv(
            hourly,
            "hourly_consumption"
        )

        write_csv(
            daily,
            "daily_consumption"
        )

        write_csv(
            monthly,
            "monthly_consumption"
        )

        write_csv(
            household,
            "household_consumption"
        )

        write_csv(
            weekday,
            "weekday_consumption"
        )

        write_csv(
            statistics,
            "overall_statistics"
        )

        write_csv(
            peak_hour,
            "peak_hour"
        )

        write_csv(
            peak_day,
            "peak_day"
        )

        write_csv(
            peak_month,
            "peak_month"
        )

        # Anomaly records
        write_csv(
            anomaly_records,
            "anomaly_detection"
        )

        # Anomaly statistical summary
        write_csv(
            anomaly_summary,
            "anomaly_summary"
        )

        # ====================================================
        # CLEANED SAMPLE
        # ====================================================

        cleaned_sample = (
            cleaned
            .orderBy("DateTime")
            .limit(1000)
        )

        write_csv(
            cleaned_sample,
            "cleaned_sample"
        )

        # ====================================================
        # FINAL MESSAGE
        # ====================================================

        print("\n========================================")
        print("SPARK ANALYTICS COMPLETED SUCCESSFULLY")
        print("========================================")

        print(
            f"Results directory: "
            f"{RESULTS_DIR}"
        )

        print("\nGenerated analytics:")

        print(
            "  ✓ Hourly consumption"
        )

        print(
            "  ✓ Daily consumption"
        )

        print(
            "  ✓ Monthly consumption"
        )

        print(
            "  ✓ Household analysis"
        )

        print(
            "  ✓ Weekday analysis"
        )

        print(
            "  ✓ Overall statistics"
        )

        print(
            "  ✓ Peak hour"
        )

        print(
            "  ✓ Peak day"
        )

        print(
            "  ✓ Peak month"
        )

        print(
            "  ✓ IQR-based anomaly detection"
        )

        print(
            "  ✓ Anomaly statistical summary"
        )

    finally:

        spark.stop()


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()