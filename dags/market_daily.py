from   airflow                      import DAG
from   airflow.operators.python     import PythonOperator
from   airflow.operators.empty      import EmptyOperator
from   airflow.decorators           import task
from   datetime                     import datetime, date, timedelta
from   pyspark.sql                  import SparkSession, functions as F
from   pyspark.sql.types            import StringType
from   delta.tables                 import DeltaTable
from   utils.market_function        import yahoo_pv, S3_save_extract
import pandas                       as pd
import requests, time, os, io


# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F

os.environ['NO_PROXY'] = '*'  #request 不用代理环境

today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

bronze=S3_save_extract("bronze",format=None)
silver=S3_save_extract("silver",format=None)

@task
def cb_market_daily_raw():
    paras = yahoo_pv(start=yesterday, end=today,ticker='SPY', ticker_list=['SPY', '510050.SS'])
    df = paras.cb_market()
    bronze.save_daily(df,'cb_market_daily_raw')
    return df.shape[0]
    
@task
def cb_currency_daily_raw():
    paras = yahoo_pv(start=yesterday, end=today,ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X'])
    df = paras.cb_currency()
    bronze.save_daily( df,'cb_currency_daily_raw')
    return df.shape[0]
    
@task 
def market_daily_currency():  
    extract_cb_market_daily_raw      =bronze.extract('cb_market_daily_raw')
    extract_cb_currency_daily_raw    =bronze.extract('cb_currency_daily_raw')
    df                               =bronze.market_currency(extract_cb_market_daily_raw, extract_cb_currency_daily_raw)
    silver.save_daily(df,'market_daily_currency')
    return df.shape[0]

@task
def market_daily_upsert_hist_currency():
    df                               =silver.merge_daily_hist('market_daily_currency')
    return df.shape[0]
    
with DAG(dag_id='market_daily',   schedule=None, start_date=datetime(2023, 1, 1),  catchup=False) as dag:
    start = EmptyOperator(task_id="start")
    cb_market_daily_raw_task                = cb_market_daily_raw()
    cb_currency_daily_raw_task              = cb_currency_daily_raw()
    market_daily_currency_task              = market_daily_currency()
    market_daily_upsert_hist_currency_task  = market_daily_upsert_hist_currency()
    end   = EmptyOperator(task_id="end")

    start >> [cb_market_daily_raw_task, cb_currency_daily_raw_task] >> market_daily_currency_task >> market_daily_upsert_hist_currency_task >> end
