import json

from kafka import KafkaConsumer
from pymongo import MongoClient


KAFKA_SERVER = "localhost:9092"
TOPIC = "energy-consumption"

MONGO_URI = "mongodb://localhost:27017"
DATABASE_NAME = "energy_analytics"
COLLECTION_NAME = "energy_consumption"

BATCH_SIZE = 1000


def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        group_id="energy-mongodb-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        )
    )


def main():

    mongo_client = MongoClient(MONGO_URI)

    database = mongo_client[DATABASE_NAME]
    collection = database[COLLECTION_NAME]

    consumer = create_consumer()

    print("Kafka → MongoDB consumer started.")
    print("Waiting for energy records...")

    batch = []
    total_records = 0

    try:

        for message in consumer:

            record = message.value
            batch.append(record)

            if len(batch) >= BATCH_SIZE:

                collection.insert_many(batch)

                total_records += len(batch)

                print(
                    f"Stored {total_records:,} records "
                    f"in MongoDB..."
                )

                batch.clear()

    except KeyboardInterrupt:

        print("\nConsumer stopped by user.")

    finally:

        # Store remaining records
        if batch:
            collection.insert_many(batch)
            total_records += len(batch)

        consumer.close()
        mongo_client.close()

        print(
            f"Total records stored: "
            f"{total_records:,}"
        )


if __name__ == "__main__":
    main()