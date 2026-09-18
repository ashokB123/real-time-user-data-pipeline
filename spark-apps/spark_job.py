"""
Spark job for the Random User pipeline.

Triggered periodically by Airflow (not a continuously-running streaming
query -- that fits an Airflow-scheduled batch model much better). Each run:

  1. Reads only the Kafka messages produced since the last run, using a
     small offsets checkpoint file so nothing is reprocessed or skipped.
  2. Parses each message's user profile fields.
  3. Uploads the raw batch as JSON to S3 (archival copy).
  4. Inserts structured rows into Postgres across 4 linked tables
     (users -> addresses / logins / contacts via user_id).
  5. Inserts full documents into MongoDB.

Requires psycopg2-binary, pymongo, and boto3 to be installed in the SPARK
image itself (not just the Airflow image) -- these are separate containers.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import psycopg2
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import (
    ArrayType, StringType, StructField, StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("random-user-spark-job")

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "random-user-topic")
OFFSETS_PATH = os.getenv("OFFSETS_PATH", "/opt/spark-apps/checkpoints/kafka_offsets.json")

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "pro-four-aws")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "customers")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "user_information")


# --------------------------------------------------------------------------
# Kafka offset checkpointing
# --------------------------------------------------------------------------
def load_starting_offsets() -> str:
    """Return a Spark-compatible startingOffsets JSON string. On first run
    (no checkpoint file yet), start from the earliest available message."""
    if os.path.exists(OFFSETS_PATH):
        with open(OFFSETS_PATH, "r") as f:
            saved = json.load(f)
        log.info("Resuming from saved offsets: %s", saved)
        return json.dumps({KAFKA_TOPIC: saved})
    log.info("No checkpoint found, starting from earliest offsets.")
    return "earliest"


def save_ending_offsets(df) -> None:
    """After processing a batch, record the max offset seen per partition
    so the next run starts right after it."""
    if df.rdd.isEmpty():
        log.info("Empty batch, not updating offsets checkpoint.")
        return

    max_offsets = (
        df.groupBy("partition")
        .agg(spark_max("offset").alias("max_offset"))
        .collect()
    )
    offsets = {str(row["partition"]): row["max_offset"] + 1 for row in max_offsets}

    os.makedirs(os.path.dirname(OFFSETS_PATH), exist_ok=True)
    with open(OFFSETS_PATH, "w") as f:
        json.dump(offsets, f)
    log.info("Saved new offsets checkpoint: %s", offsets)


# --------------------------------------------------------------------------
# Kafka read
# --------------------------------------------------------------------------
def read_new_messages(spark):
    starting_offsets = load_starting_offsets()

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", "latest")
        .load()
    )

    count = df.count()
    log.info("Read %d new message(s) from Kafka topic '%s'.", count, KAFKA_TOPIC)
    return df


# --------------------------------------------------------------------------
# Parsing: matches the envelope produced by producer.py
#   { "ingested_at": ..., "source": ..., "user": { ...random user API... } }
# --------------------------------------------------------------------------
def parse_messages(df):
    """Parse Kafka's raw JSON value column into a list of Python dicts on
    the driver. Data volumes here are small (demo-scale), so collecting to
    the driver keeps the Postgres/Mongo write logic simple and readable."""
    if df.rdd.isEmpty():
        return []

    json_strings = df.selectExpr("CAST(value AS STRING) as json_value").collect()
    records = []
    for row in json_strings:
        try:
            envelope = json.loads(row["json_value"])
            records.append(envelope)
        except (json.JSONDecodeError, TypeError) as exc:
            log.warning("Skipping unparseable message: %s", exc)
    return records


# --------------------------------------------------------------------------
# S3: raw archive
# --------------------------------------------------------------------------
def write_raw_to_s3(records: list) -> None:
    if not records:
        log.info("No records to write to S3.")
        return

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"raw_data/random_user_batch_{timestamp}.json"
    body = "\n".join(json.dumps(r) for r in records)  # one JSON object per line

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")
        log.info("Uploaded %d record(s) to s3://%s/%s", len(records), S3_BUCKET, key)
    except Exception as exc:
        log.error("Failed to upload to S3: %s", exc)
        raise


# --------------------------------------------------------------------------
# Postgres: structured, linked tables
# --------------------------------------------------------------------------
def write_to_postgres(records: list) -> None:
    if not records:
        log.info("No records to write to Postgres.")
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST, dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    cursor = conn.cursor()
    inserted = 0

    try:
        for envelope in records:
            user = envelope.get("user", {})
            login = user.get("login", {})
            name = user.get("name", {})
            location = user.get("location", {})
            street = location.get("street", {})
            dob = user.get("dob", {})
            registered = user.get("registered", {})

            # users -- insert first, get the generated id back
            cursor.execute(
                """
                INSERT INTO users (
                    uuid, gender, title, first_name, last_name, email,
                    date_of_birth, registered_date, nationality, picture_url, ingested_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (
                    login.get("uuid"),
                    user.get("gender"),
                    name.get("title"),
                    name.get("first"),
                    name.get("last"),
                    user.get("email"),
                    dob.get("date"),
                    registered.get("date"),
                    user.get("nat"),
                    user.get("picture", {}).get("large"),
                    envelope.get("ingested_at"),
                ),
            )
            row = cursor.fetchone()
            if row is None:
                # ON CONFLICT DO NOTHING means this uuid already exists; skip child rows too
                log.info("User %s already exists, skipping.", login.get("uuid"))
                continue
            user_id = row[0]

            cursor.execute(
                """
                INSERT INTO addresses (user_id, street_number, street_name, city, state, country, postcode)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    street.get("number"),
                    street.get("name"),
                    location.get("city"),
                    location.get("state"),
                    location.get("country"),
                    str(location.get("postcode", "")),
                ),
            )

            cursor.execute(
                """
                INSERT INTO logins (user_id, username, uuid, md5, sha1, sha256)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    user_id,
                    login.get("username"),
                    login.get("uuid"),
                    login.get("md5"),
                    login.get("sha1"),
                    login.get("sha256"),
                ),
            )

            cursor.execute(
                """
                INSERT INTO contacts (user_id, phone, cell)
                VALUES (%s, %s, %s)
                """,
                (user_id, user.get("phone"), user.get("cell")),
            )

            inserted += 1

        conn.commit()
        log.info("Inserted %d new user(s) into Postgres.", inserted)
    except Exception:
        conn.rollback()
        log.error("Postgres write failed, transaction rolled back.", exc_info=True)
        raise
    finally:
        cursor.close()
        conn.close()


# --------------------------------------------------------------------------
# MongoDB: full documents, no flattening needed
# --------------------------------------------------------------------------
def write_to_mongo(records: list) -> None:
    if not records:
        log.info("No records to write to MongoDB.")
        return

    client = MongoClient(MONGO_URI)
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        documents = [envelope.get("user", {}) for envelope in records]
        result = collection.insert_many(documents)
        log.info("Inserted %d document(s) into MongoDB.", len(result.inserted_ids))
    except Exception:
        log.error("MongoDB write failed.", exc_info=True)
        raise
    finally:
        client.close()


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def run():
    spark = (
        SparkSession.builder.appName("RandomUserPipeline")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0")
        .getOrCreate()
    )

    try:
        kafka_df = read_new_messages(spark)
        records = parse_messages(kafka_df)

        if not records:
            log.info("No new records this run -- nothing to write.")
            return

        write_raw_to_s3(records)
        write_to_postgres(records)
        write_to_mongo(records)

        save_ending_offsets(kafka_df)
        log.info("Run complete: %d record(s) processed.", len(records))
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
