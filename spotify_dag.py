from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner':'airflow'
}

with DAG(
    dag_id = 'spotify_scheduler',
    default_args=args,
    schedule_interval='@daily',
    start_date=days_ago(2)
) as dag:
    run_spotify=BashOperator(
        task_id='run_spotify',
        bash_command='python3 /path/of/mainscript/main.py'
        )
run_spotify        
