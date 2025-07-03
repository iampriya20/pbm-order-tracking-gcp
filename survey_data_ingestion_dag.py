from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_ID = 'airy-ceremony-463816-t4'
DB_HOST = '34.45.225.144'
DB_USER = 'postgres'
DB_NAME = 'postgres'  # Default unless you created a new one
DB_PASSWORD = Variable.get("db_password")

with DAG(
    dag_id='survey_data_ingestion_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['survey', 'ingestion', 'postgresql'],
    description='Orchestrates survey data generation and ingestion into PostgreSQL.',
) as dag:

    run_faker_survey_script = BashOperator(
        task_id='run_faker_survey_script',
        bash_command='python /home/airflow/gcs/dags/generate_survey_data.py',
        env={
            'DB_PASSWORD': DB_PASSWORD,
            'DB_HOST': DB_HOST
        }
    )

    verify_survey_data = BashOperator(
        task_id='verify_survey_data_in_postgres',
        bash_command=f"""
        echo "Verifying survey data in PostgreSQL..."
        PGPASSWORD="{DB_PASSWORD}" psql -h {DB_HOST} -U {DB_USER} -d {DB_NAME} -c "SELECT COUNT(*) FROM survey_data.survey_responses;"
        """
    )

    run_faker_survey_script >> verify_survey_data
