import yfinance as yfa   # 别名避免冲突
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime

@task
def fetch_spy_hist():
    df = yfa.Ticker('SPY').history(start='2021-02-12', end='2021-03-02')
    print(df.head(3))
    logging.info("head(3):\n%s", df.head(3))
    return df.head(3).to_dict()  # 见第2点说明
    # or: return len(df)

with DAG(
    dag_id='yf',
    schedule_interval=None,  # 新版可用 schedule=None
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    start = EmptyOperator(task_id="yahoo_market")
    yh_task = fetch_spy_hist()
    end = EmptyOperator(task_id="end")

    start >> yh_task >> end
