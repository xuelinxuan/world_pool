
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
save_bronze_parquet=S3_save_extract(niveau="bronze",format=None)

def cb_market_history_raw():
    paras = yahoo_pv(
        start='2020-01-01', end='2022-01-01',
        ticker='510050.SS', ticker_list=['SPY', '510050.SS']
    )
    df = paras.cb_market()
    save_bronze_parquet.save(df,'cb_market_history_raw')
    return df.shape[0]

def cb_market_daily_raw():
    paras = yahoo_pv(
        start=yesterday, end=today,
        ticker='SPY', ticker_list=['SPY', '510050.SS']
    )
    df = paras.cb_market()
    save_bronze_parquet.save(df,'cb_market_daily_raw')
    return df.shape[0]

def cb_currency_history_raw():
    paras = yahoo_pv(
        start='2020-01-01', end='2022-01-01',
        ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X']
    )
    df = paras.cb_currency()
    save_bronze_parquet.save(df,'cb_currency_history_raw')
    return df.shape[0]

def cb_currency_daily_raw():
    paras = yahoo_pv(
        start=yesterday, end=today,
        ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X']
    )
    df = paras.cb_currency()
    save_bronze_parquet.save( df,'cb_currency_daily_raw')
    return df.shape[0]
    
def clean(filename):
    S3=S3_save_extract("bronze", None)
    return S3.extract(filename)

with DAG(
    dag_id='market_pv',             # DAG 名称（在 Airflow UI 里显示）
    schedule_interval=None,  #手动  #'*/5 * * * *',  每 5 分钟运行一次  # 每天跑一次'@daily',   
    start_date=datetime(2023, 1, 1),        # DAG 生效开始日期
    catchup=False                           # 不补跑历史数据
) as dag:



    cb_market_history_raw_task    = PythonOperator(task_id='cb_market_history_raw',         python_callable = cb_market_history_raw,  provide_context = True)
    clean_cb_market_history_raw   = PythonOperator(task_id='clean_cb_market_history_raw',   python_callable = lambda: clean("cb_market_history_raw"))
    
    cb_market_daily_raw_task      = PythonOperator(task_id='cb_market_daily_raw',           python_callable = cb_market_daily_raw,    provide_context =True)
    clean_cb_market_daily_raw     = PythonOperator(task_id='clean_cb_market_daily_raw',     python_callable = lambda: clean("cb_market_daily_raw"))
   
    cb_currency_history_raw_task  = PythonOperator(task_id='cb_currency_history_raw',       python_callable = cb_currency_history_raw,provide_context = True)
    clean_cb_currency_history_raw = PythonOperator(task_id='clean_cb_currency_history_raw', python_callable = lambda: clean("cb_currency_history_raw"))
    
    cb_currency_daily_raw_task    = PythonOperator(task_id='cb_currency_daily_raw',         python_callable = cb_currency_daily_raw,  provide_context = True)
    clean_cb_currency_daily_raw   = PythonOperator(task_id='clean_cb_currency_daily_raw',   python_callable = lambda: clean("cb_currency_daily_raw"))


    # Set task dependencies
    [cb_market_history_raw_task   >> clean_cb_market_history_raw,
    cb_market_daily_raw_task     >> clean_cb_market_daily_raw,
    cb_currency_history_raw_task >> clean_cb_currency_history_raw,
    cb_currency_daily_raw_task   >> clean_cb_currency_daily_raw]

