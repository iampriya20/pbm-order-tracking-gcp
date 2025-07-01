# 🚚 PBM Logistics 360 - A Real-Time Pharmacy Logistics Analytics Platform

A complete, end-to-end **data engineering solution** built on **Google Cloud Platform (GCP)**. This project ingests and processes real-time event data from a specialty pharmacy's logistics chain to deliver powerful operational analytics and monitoring capabilities.

---

## 📌 Problem Statement

In most specialty pharmacies, logistics data is scattered across different systems—order management, insurance, scanners, shipping. This creates a logistics "black box" where:

- 🚫 No real-time visibility into order status or fulfillment bottlenecks.
- 🧩 Analysts manually join CSVs for outdated, error-prone reports.
- 📞 Issues are discovered *only* when a patient calls—impacting satisfaction.

**PBM Logistics 360** solves this with a unified, automated analytics platform that offers **real-time insights** and **historical accuracy**.

---

## 🎯 Data Products Delivered

1. **🖥️ Operational Command Center**  
   A real-time Streamlit dashboard to monitor active orders and logistics health.

2. **📊 Patient Satisfaction Dashboard**  
   A Looker Studio report for analyzing trends and improving service quality.

---

## 🏗️ Architecture: Lambda Design on GCP

This project implements a **Lambda Architecture** to blend both **batch** and **stream** processing for a complete view of operations.

### 1️⃣ Batch Layer – Source of Truth
- **Tool**: Cloud Composer (Airflow) + BigQuery
- **Purpose**: Create fully modeled historical datasets
- **Process**: Daily DAG jobs transform Bronze → Silver → Gold (Star Schema)

### 2️⃣ Speed Layer – Real-Time Metrics
- **Tool**: Pub/Sub + Dataflow + BigQuery
- **Purpose**: Deliver low-latency insights from recent data
- **Process**: Dataflow reads Pub/Sub events, aggregates in memory, and writes to a live BigQuery table

### 3️⃣ Serving Layer – Unified Analytics View
- **Tool**: Streamlit + BigQuery
- **Purpose**: Combine real-time and batch data for complete operational and business visibility

---

## 🧱 Tech Stack

| Component            | Tool/Service                 |
|---------------------|-----------------------------|
| **Cloud Provider**  | Google Cloud Platform (GCP) |
| **Real-Time Ingestion** | Pub/Sub, Dataflow        |
| **Batch Processing** | Cloud Composer (Airflow)   |
| **Transformation & Modeling** | BigQuery (SQL)     |
| **Transactional DB** | Cloud SQL (PostgreSQL)     |
| **Visualization**   | Streamlit, Looker Studio    |
| **Deployment**      | Cloud Run, Artifact Registry, Cloud Build |
| **Version Control** | GitHub                      |

---

## 🗂️ Data Model: Medallion Architecture in BigQuery

### 🔹 Bronze Layer (`pbm_bronze.raw_orders`)
- Immutable raw events exactly as received

### 🔸 Silver Layer (`pbm_silver.stg_order_events`)
- Cleaned, filtered, standardized
- Example: casting types, lowercasing fields, removing duplicates

### 🥇 Gold Layer (`pbm_gold`)
Final analytical tables optimized for business queries:

- `dim_patients` – Unique pseudonymized patients
- `dim_drugs` – Drug catalog
- `fact_order_lifecycle` – Order journey metrics (timestamps, duration, status)

---

## 🚀 Setup & Deployment Guide

### ✅ 1. Prerequisites
- A GCP Project with billing enabled
- Google Cloud SDK installed locally
- A GitHub repo for source control

### 🏗️ 2. Infrastructure Setup
```bash
# Enable necessary GCP APIs
gcloud services enable pubsub.googleapis.com \
    dataflow.googleapis.com \
    composer.googleapis.com \
    bigquery.googleapis.com \
    run.googleapis.com

# Create BigQuery datasets
bq mk --dataset pbm_bronze
bq mk --dataset pbm_silver
bq mk --dataset pbm_gold
