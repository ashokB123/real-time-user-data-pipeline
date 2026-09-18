"""
Airflow DAG for the Random User pipeline.

Runs on a schedule: first triggers the Kafka producer (pulls fresh fake
users from the Random User API, pushes to Kafka), then submits the Spark
job (consumes from Kafka, writes to S3 + Postgres + MongoDB).
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

sys.path.insert(0, "/opt/spark-apps")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_producer(**context):
    """Import and run the producer inline so it uses Airflow's own Python
    environment (kafka-python, requests) rather than spawning a subprocess."""
    from producer import run as producer_run
    producer_run(count=20)


with DAG(
    dag_id="random_user_pipeline",
    default_args=default_args,
    description="Fetch fake users -> Kafka -> Spark -> S3 / Postgres / MongoDB",
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "spark", "s3", "postgres", "mongodb"],
) as dag:

    produce_users = PythonOperator(
        task_id="produce_random_users",
        python_callable=run_producer,
    )

    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark-apps/spark_job.py",
        conn_id="spark_default",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.0",
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092"),
            "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "random-user-topic"),
            "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME", "pro-four-aws"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "AWS_DEFAULT_REGION": os.getenv("AWS_REGION", "eu-north-1"),
            "POSTGRES_HOST": "postgres",
            "POSTGRES_DB": "randomuser",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
            "MONGO_URI": "mongodb://root:example@mongodb:27017",
            "MONGO_DB": "customers",
            "MONGO_COLLECTION": "user_information",
        },
    )

    produce_users >> run_spark_job