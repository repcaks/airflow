from airflow import DAG
from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine
from datetime import datetime

def read_data_postgres_write_to_mysql():
    postgre_sql_stmt= "SELECT * FROM  employee_test"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_db',
        schema='airflow_test'
    )

    df = pg_hook.get_pandas_df(sql=postgre_sql_stmt)
    print(df)


with DAG("postgres_mysql_etl", start_date=datetime(2021,1,1),schedule_interval='@daily',catchup=False) as dag:

    task_read_data_postgres = PythonOperator(
        task_id="read_data_postgres",
        python_callable = read_data_postgres_write_to_mysql()
    )
