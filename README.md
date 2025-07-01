PBM Logistics 360 - A Real-Time Pharmacy Logistics Analytics Platform
This project is a complete, end-to-end data engineering solution built on Google Cloud Platform (GCP) to track and analyze the lifecycle of specialty pharmacy orders. It transforms a chaotic stream of operational events into a clean, reliable single source of truth, powering two distinct data products: a real-time operational dashboard and a business intelligence dashboard for patient feedback analysis.

🚀 Live Demo & Dashboard Preview
Live Operational Dashboard: [Link to your deployed Streamlit App on Cloud Run]

BI Patient Feedback Dashboard: [Link to your Looker Studio Report]

🎯 The Problem Statement
In a typical specialty pharmacy, logistics data is siloed across multiple systems. This creates an operational "black box" where:

There is no real-time visibility into order status or fulfillment bottlenecks.

Analysts spend days manually joining disparate CSV files to create stale, historical reports.

Problems are only discovered reactively when a patient calls, leading to a poor experience.

This project solves this by building a centralized, automated analytics platform that provides both real-time insights and historical accuracy.

🏗️ Architecture: The Lambda Architecture on GCP
This project implements a Lambda Architecture to provide a unified solution for both real-time (speed layer) and comprehensive (batch layer) data processing. This ensures our users get both up-to-the-second operational metrics and fully accurate historical analytics.

The Batch Layer (Source of Truth):

Purpose: To create a complete, accurate, and fully modeled historical record of all data.

Process: A daily job orchestrated by Cloud Composer (Airflow) executes a series of SQL scripts in BigQuery. This job transforms all of the day's raw data from the Bronze layer into a clean, aggregated Gold layer (star schema). This is our system's source of truth.

The Speed Layer (Real-Time Insights):

Purpose: To provide immediate, low-latency insights on very recent data.

Process: A dedicated, streaming Dataflow job subscribes to the Pub/Sub topic. It performs simple, in-memory aggregations (e.g., counting events in the last 5 minutes) and writes these real-time metrics to a dedicated, low-latency table in BigQuery.

The Serving Layer (Unified View):

Purpose: To present a seamless view of both real-time and historical data to the end-user.

Process: Our Streamlit dashboard intelligently queries both layers. It pulls long-term trends and historical reports from the Batch Layer's Gold tables, while simultaneously pulling the live, up-to-the-second KPIs from the Speed Layer's table.

🛠️ Tech Stack
Cloud Provider: Google Cloud Platform (GCP)

Data Ingestion:

Pub/Sub: Real-time event streaming.

Dataflow: Serverless stream processing for both raw ingest and real-time analytics.

Cloud Functions: Serverless API for survey data.

Data Warehouse: BigQuery

Transactional Database: Cloud SQL for PostgreSQL

Data Orchestration: Cloud Composer (Apache Airflow)

Dashboard & Visualization:

Streamlit: For the interactive operational dashboard.

Looker Studio: For the BI analytics dashboard.

Deployment & CI/CD:

Cloud Run: Serverless hosting for the Streamlit app.

Artifact Registry: For storing container images.

Cloud Build: For automated container builds.

GitHub: For source code management.

🗂️ Data Model (Medallion Architecture)
We use a layered approach in BigQuery for our Batch Layer to ensure data quality and maintainability.

Bronze Layer (pbm_bronze.raw_orders):

Purpose: Immutable, raw copy of all source data.

Schema: Contains the structured event data exactly as it arrived.

Silver Layer (pbm_silver.stg_order_events):

Purpose: An intermediate layer for cleaning, standardizing, and filtering data.

Transformations: Data type casting, basic quality checks, standardization (e.g., lowercasing event_type).

Gold Layer (pbm_gold):

Purpose: The final, business-ready, aggregated data model. Optimized for analytics.

Tables:

dim_patients: A dimension table with unique, pseudonymized patient information.

dim_drugs: A dimension table for unique drugs.

fact_order_lifecycle: The central fact table, with one row per order, summarizing its entire journey.

🚀 Setup and Deployment
Follow these steps to set up and run the project in your own GCP environment.

1. Prerequisites
A GCP Project with billing enabled.

Google Cloud SDK (gcloud) installed and configured locally.

A GitHub repository for your code.

2. Infrastructure Setup
Enable all necessary GCP APIs (Pub/Sub, Dataflow, BigQuery, Composer, Cloud Run, etc.).

Create the BigQuery datasets: pbm_bronze, pbm_silver, pbm_gold.

Create the pbm_bronze.raw_orders table.

Set up a Cloud Composer environment.

3. Run the Transformation
Upload the SQL scripts from the /sql_scripts directory to your Composer GCS bucket.

Upload the DAG file (pbm_pipeline_dag.py) to your Composer DAGs folder.

Trigger the Airflow DAG to build the Silver and Gold tables.

4. Deploy the Dashboard
Navigate to the /dashboard directory.

Run the deployment command:

gcloud run deploy pbm-dashboard \
  --source . \
  --region us-central1 \
  --allow-unauthenticated

✨ Future Enhancements
This platform provides a solid foundation. Future sprints could include:

Advanced Data Quality: Integrate a tool like Great Expectations or dbt tests into the Airflow DAG to run comprehensive data quality tests after each transformation step.

Machine Learning:

Build a model to predict shipping delays based on carrier, time of day, and warehouse data.

Perform sentiment analysis on the patient feedback comments to automatically categorize them as positive, negative, or neutral.

Cost Optimization: Implement table partitioning and lifecycle policies in BigQuery to automatically archive old data to GCS, reducing long-term storage costs.# pbm-order-tracking-gcp
End-to-end PBM Order Tracking project using GCP, Python, Airflow, BigQuery, Streamlit, Looker
