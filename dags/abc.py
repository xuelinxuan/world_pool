from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def say_hello():
    print("Hello Airflow!")

with DAG(
    dag_id="test_hello",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello
    )

    t1
