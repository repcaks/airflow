from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from datetime import datetime, timedelta

"""
SUCCESS SCENARIO.
- It will run the job_id=14, that is already created in databricks.
- Basically, it will trigger what that job is supposed to do (that is, run a notebook)
"""


# Define params for Run Now Operator
notebook_params = {"Variable": 5}


with DAG(
    "databricks_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "email_on_failure": False,
        "email_on_retry": False,
        "retry_delay": timedelta(minutes=2),
    },
) as dag:

    t0 = DummyOperator(
        task_id='start'
    )

    opr_run_now = DatabricksRunNowOperator(
        task_id="run_now",
        databricks_conn_id="databricks",
        job_id=14,
        notebook_params=notebook_params,
    )


    t0 >> opr_run_now