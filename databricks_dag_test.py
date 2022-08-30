from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

#Define params for Submit Run Operator
new_cluster = {
        'spark_version': '9.1.x-scala2.12',
        'node_type_id': 'Standard_L8s_v2',
        'num_workers': 1,
    }

notebook_task = {
    'notebook_path': '/Users/muchakacper8@gmail.com/showdf',
}

Define params for Run Now Operator
notebook_params = {
    
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('databricks_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks-test1',
        new_cluster=new_cluster,
        notebook_task=notebook_task
    )
    opr_run_now = DatabricksRunNowOperator(
        task_id='run_now',
        databricks_conn_id='databricks-test1',
        job_id=5,
        notebook_params=notebook_params
    )

    opr_submit_run >> opr_run_now