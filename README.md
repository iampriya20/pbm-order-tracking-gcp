# PBM Logistics Data Platform (GCP)

## Project Overview
This project demonstrates a comprehensive data platform built on Google Cloud Platform (GCP) to track Pharmaceutical Benefit Management (PBM) orders and analyze survey data. It showcases end-to-end data pipelines, medallion architecture, orchestration, and visualization.

## GCP Stack Used
- **Compute:** Dataflow, Cloud Composer (Apache Airflow), Cloud SQL (PostgreSQL)
- **Storage:** Cloud Storage (GCS), BigQuery
- **Messaging:** Pub/Sub
- **Visualization:** Streamlit, Looker Studio
- **Identity & Access Management (IAM):** For security and permissions.

## Data Pipelines

### 1. PBM Order Tracking Data Pipeline
This pipeline processes synthetic PBM order event data through a medallion architecture.
-   **Source Data Generation:** Python Faker script (`generate_pbm_orders.py`) generates event-based PBM order data (Order Placed, Insurance Validation, Order Shipped, Order Delivered events).
-   **Data Ingestion:** Generated data is published to a Pub/Sub topic (`orders-topic`).
-   **Streaming Ingestion (Bronze Layer):** Dataflow (using a custom Python pipeline/template conceptually, or a standard template) reads from the Pub/Sub subscription (`orders-topic-sub`) and streams raw event data into BigQuery's bronze layer (`pbm_bronze.raw_orders`).
-   **Medallion Architecture - BigQuery:**
    -   **Bronze:** `pbm_bronze.raw_orders` (raw, immutable event data).
    -   **Silver:** `pbm_silver.cleaned_orders_table` (cleaned, refined, often denormalized view per order, transformed from Bronze using BigQuery SQL).
    -   **Gold:** `pbm_gold.daily_order_summary` (aggregated and optimized data for dashboards, transformed from Silver using BigQuery SQL).
-   **Visualization:** Data from the Gold layer is consumed by Streamlit dashboards (developed by a team member).

### 2. Survey Data Pipeline
This pipeline processes synthetic customer survey responses into a relational database for analytics.
-   **Source Data Generation:** Python Faker script (`generate_survey_data.py`) generates survey response data.
-   **Data Ingestion:** Data is inserted directly into Cloud SQL for PostgreSQL (`pbmpostgre`) into a dedicated schema (`survey_data.survey_responses`).
-   **Visualization:** Data from PostgreSQL is used to create interactive dashboards in Looker Studio.

## Orchestration
Apache Airflow, managed by Google Cloud Composer, orchestrates the entire PBM data pipeline:
-   **DAG:** `pbm_order_ingestion_pipeline`
-   **Tasks:**
    -   `run_faker_script`: Executes `generate_pbm_orders.py` to produce and publish data.
    -   `launch_dataflow_job`: (Conceptually) Launches the Dataflow streaming job to ingest data into Bronze. (Note: Due to a specific project-level issue, this task is currently mocked in the DAG to allow the pipeline to proceed, but ideally, it would trigger the actual Dataflow job).
    -   `transform_to_silver`: Runs BigQuery SQL to process data from Bronze to Silver.
    -   `transform_to_gold`: Runs BigQuery SQL to aggregate data from Silver to Gold.

A separate DAG (`survey_data_ingestion_pipeline`) orchestrates the Faker script for survey data and a verification step into PostgreSQL.

## Dashboards
-   **Streamlit:** Connected to BigQuery Gold layer for PBM order tracking (developed by a team member).
-   **Looker Studio:** Connected to Cloud SQL for PostgreSQL for Customer Survey Satisfaction insights.

## Security Considerations
-   **IAM:** Granular permissions are applied to service accounts (e.g., Composer service account has specific roles for Dataflow, BigQuery, Pub/Sub, Cloud SQL access).
-   **Data Encryption:** Data at rest and in transit is encrypted by default in GCP.
-   **Network Access:** Cloud SQL Public IP access is restricted to authorized IP addresses (or `0.0.0.0/0` for testing, to be restricted in production). Composer is configured with Public IP web server access for ease of access during development, to be restricted in production.
-   **Secret Management:** Database passwords are stored as Airflow Variables (recommended over hardcoding).

## How to Run
(Detailed instructions on setting up GCP, local environment, and triggering DAGs will be added here for full project deployment).

---
*This README reflects the current state of the project development and highlights key components and architectural decisions.*