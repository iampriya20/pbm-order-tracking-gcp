import subprocess
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

# --- DAG Configuration ---
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- Google Cloud Project Details ---
PROJECT_ID = 'airy-ceremony-463816-t4'
REGION = 'us-central1'

# --- BigQuery Table Details ---
BRONZE_TABLE = f'{PROJECT_ID}.pbm_bronze.raw_orders'
SILVER_TABLE = f'{PROJECT_ID}.pbm_silver.cleaned_orders_table'
GOLD_TABLE = f'{PROJECT_ID}.pbm_gold.daily_order_summary'


# --- Python Function: Launch Dataflow Job (Legacy Template) ---
def _launch_dataflow_job(ti):
    job_name = f"pbm-ingest-job-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
    
    command = [
        "gcloud", "dataflow", "jobs", "run", job_name,
        "--gcs-location", "gs://dataflow-templates-us-central1/latest/PubSub_Subscription_to_BigQuery",
        "--region", REGION,
        "--project", PROJECT_ID,
        "--parameters",
        f"inputSubscription=projects/{PROJECT_ID}/subscriptions/orders-topic-sub,"
        f"outputTableSpec={BRONZE_TABLE}"
    ]

    print(f"Launching Dataflow job: {' '.join(command)}")
    process = subprocess.run(command, capture_output=True, text=True)

    if process.returncode != 0:
        print("STDOUT:", process.stdout)
        print("STDERR:", process.stderr)
        raise Exception(f"Dataflow job launch failed with exit code {process.returncode}")
    else:
        print("Dataflow job launched successfully.")
        print("STDOUT:", process.stdout)


# --- Python Function: Transform to Silver ---
def _transform_to_silver(ti):
    sql_query = f"""
    CREATE OR REPLACE TABLE `{SILVER_TABLE}` AS
    SELECT
        order_id,
        MAX(CASE WHEN event_type = "Order_Placed" THEN event_timestamp END) AS order_placed_timestamp,
        MAX(CASE WHEN event_type = "Insurance_Validation" THEN event_timestamp END) AS insurance_validated_timestamp,
        MAX(CASE WHEN event_type = "Order_Shipped" THEN event_timestamp END) AS order_shipped_timestamp,
        MAX(CASE WHEN event_type = "Order_Delivered" THEN event_timestamp END) AS order_delivered_timestamp,
        MAX(patient_name) AS patient_name,
        MAX(drug_name) AS drug_name,
        MAX(quantity) AS quantity,
        MAX(pharmacy_id) AS pharmacy_id,
        MAX(prescription_id) AS prescription_id,
        MAX(total_cost) AS total_cost,
        MAX(delivery_address_street) AS delivery_address_street,
        MAX(delivery_address_city) AS delivery_address_city,
        MAX(delivery_address_state) AS delivery_address_state,
        MAX(delivery_address_zip_code) AS delivery_address_zip_code,
        MAX(insurance_provider) AS insurance_provider,
        MAX(claim_status) AS final_claim_status,
        MAX(tracking_number) AS tracking_number,
        ARRAY_AGG(STRUCT(event_timestamp, status_after_event) ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)].status_after_event AS current_order_status
    FROM `{BRONZE_TABLE}`
    GROUP BY order_id
    """

    command = [
        "bq", "query",
        "--project_id", PROJECT_ID,
        "--use_legacy_sql=false",
        sql_query
    ]

    print("Running Silver transformation...")
    process = subprocess.run(command, capture_output=True, text=True)

    if process.returncode != 0:
        print("STDOUT:", process.stdout)
        print("STDERR:", process.stderr)
        raise Exception("Silver transformation failed.")
    else:
        print("Silver transformation completed.")
        print("STDOUT:", process.stdout)


# --- Python Function: Transform to Gold ---
def _transform_to_gold(ti):
    sql_query = f"""
    CREATE OR REPLACE TABLE `{GOLD_TABLE}` AS
    SELECT
        CAST(order_placed_timestamp AS DATE) AS order_date,
        COUNT(DISTINCT order_id) AS orders_placed_daily,
        COUNT(DISTINCT CASE WHEN current_order_status = "Shipped" THEN order_id END) AS orders_shipped_daily,
        COUNT(DISTINCT CASE WHEN current_order_status IN ("Pending", "Processing", "On Hold") THEN order_id END) AS current_open_orders,
        AVG(TIMESTAMP_DIFF(order_shipped_timestamp, order_placed_timestamp, HOUR)) AS avg_time_to_ship_hours,
        COUNT(DISTINCT CASE WHEN final_claim_status = "Denied" THEN order_id END) AS orders_rejected_daily
    FROM `{SILVER_TABLE}`
    WHERE order_placed_timestamp IS NOT NULL
    GROUP BY order_date
    ORDER BY order_date DESC
    """

    command = [
        "bq", "query",
        "--project_id", PROJECT_ID,
        "--use_legacy_sql=false",
        sql_query
    ]

    print("Running Gold transformation...")
    process = subprocess.run(command, capture_output=True, text=True)

    if process.returncode != 0:
        print("STDOUT:", process.stdout)
        print("STDERR:", process.stderr)
        raise Exception("Gold transformation failed.")
    else:
        print("Gold transformation completed.")
        print("STDOUT:", process.stdout)


# --- DAG Definition ---
with DAG(
    dag_id='pbm_order_ingestion_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['pbm', 'ingestion', 'dataflow', 'bigquery'],
    description='Orchestrates PBM order data generation, Dataflow ingestion, and BigQuery transformations.',
) as dag:

    run_faker_script = BashOperator(
        task_id='run_faker_script',
        bash_command='python /home/airflow/gcs/dags/generate_pbm_orders.py',
    )

    launch_dataflow_job = PythonOperator(
        task_id='launch_dataflow_job',
        python_callable=_launch_dataflow_job,
    )

    transform_to_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=_transform_to_silver,
    )

    transform_to_gold = PythonOperator(
        task_id='transform_to_gold',
        python_callable=_transform_to_gold,
    )

    run_faker_script >> launch_dataflow_job >> transform_to_silver >> transform_to_gold
