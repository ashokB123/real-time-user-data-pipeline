Random User API

│

▼

Kafka Producer (Airflow task)

│

▼

Kafka Topic (random-user-topic)

│

▼

Spark Structured Job (Airflow task, triggered on schedule)

│

├──────────────► AWS S3 (raw JSON archive)

├──────────────► PostgreSQL (4 normalized, linked tables)

└──────────────► MongoDB (flexible document store)





Airflow runs the whole flow every 10 minutes: first the producer fetches a fresh batch of fake users and pushes them to Kafka, then Spark reads only the new messages (using an offset checkpoint so nothing is reprocessed or skipped) and fans them out to all three destinations.



\## Stack



\- \*\*Kafka + Zookeeper + Schema Registry\*\* — message streaming backbone

\- \*\*Apache Spark\*\* (standalone cluster: master + worker) — batch processing of each Kafka run

\- \*\*Apache Airflow\*\* — orchestration and scheduling

\- \*\*PostgreSQL\*\* — structured, normalized storage (4 tables linked by `user\_id`)

\- \*\*MongoDB\*\* — raw document storage

\- \*\*AWS S3\*\* — raw JSON archive of every batch

\- \*\*Docker Compose\*\* — runs all 9 services locally



\## Postgres schema



Unlike a flat single-table approach, this schema properly normalizes the data into 4 linked tables:



\- `users` — core profile (name, email, DOB, nationality, etc.)

\- `addresses` — linked via `user\_id`

\- `logins` — linked via `user\_id`

\- `contacts` — linked via `user\_id`



This makes the data actually queryable/joinable, which a flat design doesn't support.



\## Setup



1\. Clone this repo

2\. Create a `.env` file in the project root with:



AWS\_ACCESS\_KEY\_ID=your\_key

AWS\_SECRET\_ACCESS\_KEY=your\_secret

AWS\_REGION=your\_region

S3\_BUCKET\_NAME=your\_bucket



3\. Build the Airflow image:



docker compose build webserver



4\. Start the core services:



docker compose up -d postgres pgadmin zookeeper broker schema-registry spark-master spark-worker mongodb



5\. Apply the Postgres schema:



docker compose exec postgres psql -U airflow -d postgres -c "CREATE DATABASE randomuser OWNER airflow;"

Get-Content schema.sql | docker compose exec -T postgres psql -U airflow -d randomuser



6\. Start Airflow:



docker compose up -d webserver scheduler



7\. In the Airflow UI (`localhost:8080`, login `airflow`/`airflow`), add a Spark connection: Admin → Connections → `spark\_default`, host `spark://spark-master`, port `7077`

8\. Unpause and trigger the `random\_user\_pipeline` DAG



\## Challenges solved along the way



This project involved debugging a number of real-world infrastructure issues:



\- Stale Debian package repositories causing `apt-get` failures in the base image

\- A missing C compiler causing pip package builds to fail

\- A deprecated Docker Hub image tag (`bitnami/spark:latest`) requiring a pin to a specific, compatible version

\- A Spark/PySpark version mismatch (protocol-level `InvalidClassException`) between the cluster and driver

\- Airflow/Spark provider version compatibility constraints

\- A database migration race condition between the Airflow webserver and scheduler on startup

\- Postgres unique-constraint conflicts causing batch failures and reprocessing loops



\## Verified working



\- \*\*S3\*\*: raw JSON batches accumulating in `raw\_data/`

\- \*\*PostgreSQL\*\*: 1500+ linked rows across all 4 tables

\- \*\*MongoDB\*\*: 4800+ documents



\## Tech skills demonstrated



Kafka, Spark (Structured batch processing), Airflow (DAG orchestration, custom operators), PostgreSQL (schema design, transactions), MongoDB, AWS S3 (boto3), Docker Compose (multi-service orchestration), Python

