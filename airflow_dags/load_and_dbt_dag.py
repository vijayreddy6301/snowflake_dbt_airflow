from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'load_and_dbt_dag',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:

    # Task 1: Load data into Snowflake
    load_to_snowflake = BashOperator(
        task_id='load_data_to_snowflake',
        bash_command='source /home/hp/airflow_venv/bin/activate && python /home/hp/airflow_venv/airflow_proj/Scripts/main.py',
        env={'AIRFLOW_HOME': '/home/hp/airflow'},
        do_xcom_push=True
    )

    # Task 2: Run dbt models
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='/home/hp/airflow_venv/bin/dbt run',
        cwd='/home/hp/airflow_venv/airflow_proj'
    )

    # Define task dependencies
    load_to_snowflake >> run_dbt_models
