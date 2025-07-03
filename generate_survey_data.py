import psycopg2
from faker import Faker
import random
import time
import datetime
import os

# Initialize Faker
fake = Faker()

# --- PostgreSQL Database Configuration ---
# Retrieve sensitive info from environment variables.
# You MUST set DB_PASSWORD in your terminal BEFORE running the script:
# On macOS/Linux: export DB_PASSWORD="Qwerty"
# On Windows (Command Prompt): set DB_PASSWORD="Qwerty"
# On Windows (PowerShell): $env:DB_PASSWORD="Qwerty"

DB_HOST = "34.45.225.144"       # Your Cloud SQL Public IP
DB_NAME = "postgres"            # Default database name for PostgreSQL (initial connection)
DB_USER = "postgres"            # Default user for PostgreSQL
DB_PASSWORD = os.environ.get("DB_PASSWORD") # Reads from environment variable

# --- Schema and Table Name ---
SCHEMA_NAME = "survey_data" # <-- NEW: The schema we want to use
TABLE_NAME = "survey_responses"

# --- Basic check if password is set (good practice) ---
if DB_PASSWORD is None:
    print("Error: DB_PASSWORD environment variable not set. Please set it before running the script.")
    exit(1)

# --- Function to generate a single fake survey response ---
def generate_fake_survey_response():
    response = {
        "survey_id": fake.uuid4(),
        "respondent_id": fake.uuid4(),
        "submission_date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
        "age_group": random.choice(["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]),
        "gender": random.choice(["Male", "Female", "Non-binary", "Prefer not to say"]),
        "satisfaction_rating": random.randint(1, 5), # 1-5 scale
        "comment": fake.sentence(nb_words=10),
        "drug_usage_frequency": random.choice(["Daily", "Weekly", "Monthly", "Rarely", "Never"]),
        "side_effects_experienced": random.choice([True, False]),
        "healthcare_provider_rating": random.randint(1, 5)
    }
    return response

# --- Function to insert survey data into PostgreSQL ---
def insert_survey_data(survey_data):
    conn = None
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        # 1. Create the schema if it doesn't exist
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"
        cur.execute(create_schema_sql)
        conn.commit()

        # 2. Create the table within the specified schema if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            survey_id VARCHAR(255) PRIMARY KEY,
            respondent_id VARCHAR(255),
            submission_date DATE,
            age_group VARCHAR(50),
            gender VARCHAR(50),
            satisfaction_rating INTEGER,
            comment TEXT,
            drug_usage_frequency VARCHAR(50),
            side_effects_experienced BOOLEAN,
            healthcare_provider_rating INTEGER
        );
        """
        cur.execute(create_table_sql)
        conn.commit() # Commit table creation

        # 3. SQL to insert data into the table within the specified schema
        insert_sql = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
            survey_id, respondent_id, submission_date, age_group, gender,
            satisfaction_rating, comment, drug_usage_frequency,
            side_effects_experienced, healthcare_provider_rating
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cur.execute(insert_sql, (
            survey_data['survey_id'], survey_data['respondent_id'], survey_data['submission_date'],
            survey_data['age_group'], survey_data['gender'], survey_data['satisfaction_rating'],
            survey_data['comment'], survey_data['drug_usage_frequency'],
            survey_data['side_effects_experienced'], survey_data['healthcare_provider_rating']
        ))
        conn.commit()
        print(f"Inserted survey_id: {survey_data['survey_id']} into {SCHEMA_NAME}.{TABLE_NAME}")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or inserting data: {error}")
        if conn:
            conn.rollback() # Rollback on error
    finally:
        if conn:
            cur.close()
            conn.close()

# --- Main execution block ---
if __name__ == "__main__":
    print("--- Starting Survey Data Generation and DB Insertion ---")
    num_responses = 5000 # Generate 5000 responses
    for i in range(num_responses):
        survey_data = generate_fake_survey_response()
        insert_survey_data(survey_data)
        time.sleep(0.05) # Small delay to avoid overwhelming the DB or network

    print("--- Survey Data Generation and DB Insertion Complete ---")