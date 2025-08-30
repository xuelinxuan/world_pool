
from   airflow                      import DAG
from   airflow.operators.python     import PythonOperator
from   airflow.operators.empty      import EmptyOperator
from   datetime                     import datetime,date, timedelta
from   pyspark.sql                  import SparkSession, functions as F
from   delta.tables                 import DeltaTable
from   utils.market_pv              import data_clean, yahoo_pv, S3_save_extract
import requests, time, os,io
import pandas as pd

# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F

os.environ['NO_PROXY'] = '*'  #request 不用代理环境

today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

# # format_tppe:  parquet:None,snappy,gzip
extract=S3_save_extract(niveau="bronze",format=None)


def extract(filename):
    S3=S3_save_extract("bronze", None)
    return S3.extract(filename)
    
with DAG(
    dag_id='bronze_market',             # DAG 名称（在 Airflow UI 里显示）
    schedule_interval=None,  #手动  #'*/5 * * * *',  每 5 分钟运行一次  # 每天跑一次'@daily',   
    start_date=datetime(2023, 1, 1),        # DAG 生效开始日期
    catchup=False                           # 不补跑历史数据
) as dag:

    start = EmptyOperator(task_id='start')

    cb_market_history_raw_task   = PythonOperator(
        task_id         ='extract_cb_market_history_raw',
        python_callable = lambda: extract("cb_market_history_raw"),
        )

    cb_market_daily_raw_task     = PythonOperator(
        task_id         ='extract_cb_market_daily_raw',
        python_callable = lambda: extract("cb_market_daily_raw"),
        )

    cb_currency_history_raw_task = PythonOperator(
        task_id         ='extract_cb_currency_history_raw',
        python_callable = lambda: extract("cb_currency_history_raw"),
         )

    cb_currency_daily_raw_task   = PythonOperator(
        task_id         ='extract_cb_currency_daily_raw',
        python_callable = lambda: extract("cb_currency_daily_raw"),
        )

    end = EmptyOperator(task_id='end')

    # Set task dependencies
    start >> [cb_market_history_raw_task, cb_market_daily_raw_task, cb_currency_history_raw_task, cb_currency_daily_raw_task]>> end
