from   airflow                      import DAG
from   airflow.operators.python     import PythonOperator
from   airflow.operators.empty      import EmptyOperator
from   datetime                     import datetime,date, timedelta
from   pyspark.sql                  import SparkSession, functions as F
from   delta.tables                 import DeltaTable
from   utils.market_pv              import yahoo_pv, SaveS3_niveau_format
import requests, time, os

import pandas as pd

# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F

os.environ['NO_PROXY'] = '*'  #request 不用代理环境

today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


# currency_history   =yahoo_pv(start='2020-01-01'   , end='2022-01-01', ticker='CNY=X'     , ticker_list =["CNY=X","EURCHF=X"])
# currency_daily     =yahoo_pv(start=yesterday      , end=today       , ticker='CNY=X'     , ticker_list =["CNY=X","EURCHF=X"])
# market_history     =yahoo_pv(start='2020-01-01'   , end='2022-01-01', ticker='510050.SS' , ticker_list =["SPY"  ,"510050.SS"])
# market_daily       =yahoo_pv(start=yesterday      , end=today       , ticker='SPY'       , ticker_list =["SPY"  ,"510050.SS"])
# # format_tppe:  parquet:None,snappy,gzip
save_bronze_parquet=SaveS3_niveau_format(niveau="bronze",format=None)


# cb_market_history_raw   = market_history.cb_market()
# cb_market_history_raw   = save_bronze_parquet.name(cb_market_history_raw, "cb_market_history_raw")

# cb_market_daily_raw     = market_daily.cb_market()
# cb_market_daily_raw     = save_bronze_parquet.name(cb_market_daily_raw, "cb_market_daily_raw")

# cb_currency_history_raw = currency_history.cb_currency()
# cb_currency_history_raw = save_bronze_parquet.name(cb_currency_history_raw, "cb_currency_history_raw")

# cb_currency_daily_raw   = currency_daily.cb_currency()
# cb_currency_daily_raw   = save_bronze_parquet.name(cb_currency_daily_raw, "cb_currency_daily_raw")

def cb_market_history_raw():
    paras = yahoo_pv(
        start='2020-01-01', end='2022-01-01',
        ticker='510050.SS', ticker_list=['SPY', '510050.SS']
    )
    df = paras.cb_market()
    save_bronze_parquet.name(df, "cb_market_history_raw")
    return df.shape[0]

def cb_market_daily_raw():
    paras = yahoo_pv(
        start=yesterday, end=today,
        ticker='SPY', ticker_list=['SPY', '510050.SS']
    )
    df = paras.cb_market()
    save_bronze_parquet.name(df, "cb_market_daily_raw")
    return df.shape[0]

def cb_currency_history_raw():
    paras = yahoo_pv(
        start='2020-01-01', end='2022-01-01',
        ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X']
    )
    df = paras.cb_currency()
    save_bronze_parquet.name(df, "cb_currency_history_raw")
    return df.shape[0]

def cb_currency_daily_raw():
    paras = yahoo_pv(
        start=yesterday, end=today,
        ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X']
    )
    df = paras.cb_currency()
    save_bronze_parquet.name(df, "cb_currency_daily_raw")
    return df.shape[0]



with DAG(
    dag_id='bronze_market',             # DAG 名称（在 Airflow UI 里显示）
    schedule_interval=None,  #手动  #'*/5 * * * *',  每 5 分钟运行一次  # 每天跑一次'@daily',   
    start_date=datetime(2023, 1, 1),        # DAG 生效开始日期
    catchup=False                           # 不补跑历史数据
) as dag:

    start = EmptyOperator(task_id='start')

    cb_market_history_raw_task   = PythonOperator(
        task_id         ='cb_market_history_raw',
        python_callable = cb_market_history_raw,
        provide_context = True )

    cb_market_daily_raw_task     = PythonOperator(
        task_id         ='cb_market_daily_raw',
        python_callable =cb_market_daily_raw,
        provide_context =True )

    cb_currency_history_raw_task = PythonOperator(
        task_id         ='cb_currency_history_raw',
        python_callable =cb_currency_history_raw,
        provide_context =True )

    cb_currency_daily_raw_task   = PythonOperator(
        task_id         ='cb_currency_daily_raw',
        python_callable =cb_currency_daily_raw,
        provide_context =True )

    end = EmptyOperator(task_id='end')


    # Set task dependencies
    start >> [cb_market_history_raw_task, cb_market_daily_raw_task, cb_currency_history_raw_task, cb_currency_daily_raw_task]>> end
