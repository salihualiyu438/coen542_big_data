import csv
import json
import glob
import os

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC = "energy-consumption"

DATA_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data"
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def convert_energy_value(value):
    """
    Convert the energy-consumption value to float.

    Missing values such as 'Null', 'null', or empty
    strings are represented as None.
    """

    value = value.strip()

    if value.lower() in ("null", "none", ""):
        return None

    return float(value)


def stream_csv_files(producer):

    csv_files = glob.glob(
        os.path.join(DATA_FOLDER, "*.csv")
    )

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {DATA_FOLDER}"
        )

    print(f"Found {len(csv_files)} CSV file(s).")

    total_records = 0
    missing_energy = 0

    for file_path in csv_files:

        filename = os.path.basename(file_path)

        print(f"\nProcessing: {filename}")

        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:

                energy_value = convert_energy_value(
                    row["KWH/hh (per half hour) "]
                )

                if energy_value is None:
                    missing_energy += 1

                record = {
                    "LCLid": row["LCLid"].strip(),
                    "stdorToU": row["stdorToU"].strip(),
                    "DateTime": row["DateTime"].strip(),
                    "energy_kwh": energy_value
                }

                producer.send(
                    TOPIC,
                    value=record
                )

                total_records += 1

                if total_records % 10000 == 0:

                    producer.flush()

                    print(
                        f"Sent {total_records:,} records..."
                    )

    producer.flush()

    print("\n================================")
    print("Kafka ingestion completed")
    print("================================")
    print(
        f"Total records sent: "
        f"{total_records:,}"
    )
    print(
        f"Records with missing energy: "
        f"{missing_energy:,}"
    )


if __name__ == "__main__":

    producer = create_producer()

    try:
        stream_csv_files(producer)

    finally:
        producer.close()
        print("Kafka producer closed.")