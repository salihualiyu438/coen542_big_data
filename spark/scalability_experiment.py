import os
import sys
import time
from pathlib import Path


# ============================================================
# COEN542 ENERGY ANALYTICS
# SCALABILITY EXPERIMENT
# ============================================================

# ------------------------------------------------------------
# JAVA 17
# ------------------------------------------------------------

JAVA_HOME = r"C:\Program Files\Java\jdk-17"

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = JAVA_HOME + r"\bin;" + os.environ.get("PATH", "")


# ------------------------------------------------------------
# PYTHON
# ------------------------------------------------------------

# Use the Python interpreter that is actually running this script.
PYTHON_EXE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE

os.environ["HADOOP_HOME"] = r"C:\hadoop"


# ------------------------------------------------------------
# PROJECT PATHS
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

RESULTS_DIR = (
    BASE_DIR
    / "results"
    / "scalability_experiment"
)

RESULTS_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ------------------------------------------------------------
# PYSPARK
# ------------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    sum as spark_sum,
    abs as spark_abs,
    pmod,
    xxhash64,
    concat_ws,
    hour
)


# ------------------------------------------------------------
# CREATE SPARK SESSION
# ------------------------------------------------------------

def create_spark_session():

    spark = (
        SparkSession.builder

        .appName(
            "COEN542_Scalability_Experiment"
        )

        .master("local[*]")

        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0"
        )

        # Explicitly tell Spark which Python to use.
        .config(
            "spark.pyspark.python",
            PYTHON_EXE
        )

        .config(
            "spark.pyspark.driver.python",
            PYTHON_EXE
        )

        # Disable UI because this experiment does not need it.
        .config(
            "spark.ui.enabled",
            "false"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ------------------------------------------------------------
# LOAD MONGODB DATA
# ------------------------------------------------------------

def load_data(spark):

    print()
    print("=" * 60)
    print("LOADING ENERGY DATA FROM MONGODB")
    print("=" * 60)

    start = time.perf_counter()

    df = (
        spark.read
        .format("mongodb")
        .option(
            "spark.mongodb.read.connection.uri",
            "mongodb://127.0.0.1:27017"
        )
        .option(
            "spark.mongodb.read.database",
            "energy_analytics"
        )
        .option(
            "spark.mongodb.read.collection",
            "energy_consumption"
        )
        .load()
    )

    total_records = df.count()

    elapsed = time.perf_counter() - start

    print(
        f"Total records available: "
        f"{total_records:,}"
    )

    print(
        f"Initial MongoDB read time: "
        f"{elapsed:.3f} seconds"
    )

    return df


# ------------------------------------------------------------
# PREPARE DATA
# ------------------------------------------------------------

def prepare_data(df):

    print()
    print("Preparing dataset...")

    df = (
        df
        .select(
            "LCLid",
            "DateTime",
            "energy_kwh",
            "stdorToU"
        )

        .withColumn(
            "energy_kwh",
            col("energy_kwh").cast("double")
        )

        .withColumn(
            "energy_kwh",
            when(
                col("energy_kwh").isNull(),
                0.0
            ).otherwise(
                col("energy_kwh")
            )
        )
    )

    return df


# ------------------------------------------------------------
# CREATE DETERMINISTIC SUBSET
# ------------------------------------------------------------

def create_subset(df, percentage):

    if percentage == 100:
        return df

    return df.filter(
        pmod(
            spark_abs(
                xxhash64(
                    concat_ws(
                        "||",
                        col("LCLid"),
                        col("DateTime")
                    )
                )
            ),
            100
        ) < percentage
    )


# ------------------------------------------------------------
# ANALYTICS WORKLOAD
# ------------------------------------------------------------

def run_analytics(df):

    working_df = (
        df.withColumn(
            "hour",
            hour(col("DateTime"))
        )
    )

    hourly = (
        working_df
        .groupBy("hour")
        .agg(
            spark_sum(
                "energy_kwh"
            ).alias(
                "total_energy_kwh"
            )
        )
    )

    household = (
        working_df
        .groupBy("LCLid")
        .agg(
            spark_sum(
                "energy_kwh"
            ).alias(
                "total_energy_kwh"
            )
        )
    )

    overall = (
        working_df
        .agg(
            spark_sum(
                "energy_kwh"
            ).alias(
                "total_energy_kwh"
            )
        )
    )

    # Force complete execution.
    hourly.count()
    household.count()
    overall.collect()


# ------------------------------------------------------------
# SAVE RESULTS
# ------------------------------------------------------------

def save_results(results):

    csv_path = (
        RESULTS_DIR
        / "scalability_results.csv"
    )

    with open(
        csv_path,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            "data_percentage,"
            "records,"
            "execution_time_seconds,"
            "throughput_records_per_second\n"
        )

        for result in results:

            f.write(
                f"{result['percentage']},"
                f"{result['records']},"
                f"{result['time']:.6f},"
                f"{result['throughput']:.6f}\n"
            )

    print()
    print(
        f"Saved: {csv_path}"
    )


# ------------------------------------------------------------
# CREATE GRAPHS
# ------------------------------------------------------------

def create_graphs(results):

    import matplotlib.pyplot as plt

    percentages = [
        x["percentage"]
        for x in results
    ]

    times = [
        x["time"]
        for x in results
    ]

    throughputs = [
        x["throughput"]
        for x in results
    ]

    # --------------------------------------------------------
    # Execution time
    # --------------------------------------------------------

    plt.figure(
        figsize=(9, 6)
    )

    plt.plot(
        percentages,
        times,
        marker="o"
    )

    plt.xlabel(
        "Data Size (%)"
    )

    plt.ylabel(
        "Median Execution Time (seconds)"
    )

    plt.title(
        "COEN542 Scalability Experiment - Execution Time"
    )

    plt.xticks(
        percentages
    )

    plt.grid(
        True,
        alpha=0.3
    )

    plt.tight_layout()

    execution_path = (
        RESULTS_DIR
        / "scalability_execution_time.png"
    )

    plt.savefig(
        execution_path,
        dpi=300
    )

    plt.close()

    # --------------------------------------------------------
    # Throughput
    # --------------------------------------------------------

    plt.figure(
        figsize=(9, 6)
    )

    plt.plot(
        percentages,
        throughputs,
        marker="o"
    )

    plt.xlabel(
        "Data Size (%)"
    )

    plt.ylabel(
        "Median Throughput (records/second)"
    )

    plt.title(
        "COEN542 Scalability Experiment - Throughput"
    )

    plt.xticks(
        percentages
    )

    plt.grid(
        True,
        alpha=0.3
    )

    plt.tight_layout()

    throughput_path = (
        RESULTS_DIR
        / "scalability_throughput.png"
    )

    plt.savefig(
        throughput_path,
        dpi=300
    )

    plt.close()

    print(
        f"Saved: {execution_path}"
    )

    print(
        f"Saved: {throughput_path}"
    )


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():

    print()
    print("=" * 60)
    print("COEN542 ENERGY ANALYTICS")
    print("MANDATORY SCALABILITY EXPERIMENT")
    print("=" * 60)

    print()
    print(
        f"Python used:\n{PYTHON_EXE}"
    )

    print()
    print(
        f"Java:\n{JAVA_HOME}"
    )

    spark = None

    try:

        spark = create_spark_session()

        # ----------------------------------------------------
        # LOAD DATA
        # ----------------------------------------------------

        df = load_data(spark)

        # ----------------------------------------------------
        # PREPARE
        # ----------------------------------------------------

        df = prepare_data(df)

        df = df.cache()

        total_records = df.count()

        print()
        print(
            f"Prepared records: "
            f"{total_records:,}"
        )

        # ----------------------------------------------------
        # WARM-UP
        # ----------------------------------------------------

        print()
        print(
            "=" * 60
        )

        print(
            "PERFORMING SPARK WARM-UP"
        )

        print(
            "=" * 60
        )

        warmup_df = create_subset(
            df,
            25
        )

        run_analytics(
            warmup_df
        )

        print(
            "Warm-up completed."
        )

        # ----------------------------------------------------
        # EXPERIMENT
        # ----------------------------------------------------

        percentages = [
            25,
            50,
            100
        ]

        repetitions = 3

        results = []

        print()
        print(
            "=" * 60
        )

        print(
            "STARTING SCALABILITY EXPERIMENT"
        )

        print(
            f"Runs per data size: {repetitions}"
        )

        print(
            "=" * 60
        )

        for percentage in percentages:

            print()
            print(
                "-" * 60
            )

            print(
                f"DATA SIZE: {percentage}%"
            )

            print(
                "-" * 60
            )

            subset = create_subset(
                df,
                percentage
            )

            records = subset.count()

            print(
                f"Records: {records:,}"
            )

            run_times = []

            for run_number in range(
                1,
                repetitions + 1
            ):

                start = (
                    time.perf_counter()
                )

                run_analytics(
                    subset
                )

                elapsed = (
                    time.perf_counter()
                    - start
                )

                run_times.append(
                    elapsed
                )

                print(
                    f"Run {run_number}: "
                    f"{elapsed:.3f} seconds"
                )

            # ------------------------------------------------
            # Median
            # ------------------------------------------------

            sorted_times = sorted(
                run_times
            )

            median_time = sorted_times[
                len(sorted_times) // 2
            ]

            throughput = (
                records /
                median_time
            )

            result = {
                "percentage": percentage,
                "records": records,
                "time": median_time,
                "throughput": throughput
            }

            results.append(
                result
            )

            print()
            print(
                f"Median time: "
                f"{median_time:.3f} seconds"
            )

            print(
                f"Throughput: "
                f"{throughput:,.2f} records/second"
            )

        # ----------------------------------------------------
        # SUMMARY
        # ----------------------------------------------------

        print()
        print(
            "=" * 75
        )

        print(
            "FINAL SCALABILITY RESULTS"
        )

        print(
            "=" * 75
        )

        print(
            f"{'Size':<10}"
            f"{'Records':<18}"
            f"{'Median Time':<18}"
            f"{'Throughput':<20}"
        )

        print(
            "-" * 75
        )

        for result in results:

            print(
                f"{result['percentage']}%"
                f"{'':<7}"
                f"{result['records']:<18,}"
                f"{result['time']:<18.3f}"
                f"{result['throughput']:<20,.2f}"
            )

        print(
            "=" * 75
        )

        # ----------------------------------------------------
        # SAVE
        # ----------------------------------------------------

        save_results(
            results
        )

        create_graphs(
            results
        )

        print()
        print(
            "=" * 60
        )

        print(
            "SCALABILITY EXPERIMENT COMPLETED"
        )

        print(
            "=" * 60
        )

        print()
        print(
            f"Results directory:\n"
            f"{RESULTS_DIR}"
        )

    finally:

        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()