from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def hello():
    print("Airflow python script test")

args = {
    "owner": "Test",
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id = 'Python-test',
    default_args=args,
    schedule_interval='@daily'
)

with dag:
    hello_world = PythonOperator(
        task_id='hello',
        python_callable=hello,
    )

