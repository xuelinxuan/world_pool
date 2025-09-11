
from   airflow                      import DAG
from   airflow.operators.python     import PythonOperator
from   airflow.operators.empty      import EmptyOperator
from   airflow.decorators           import task
from   datetime                     import datetime, date, timedelta
from   pyspark.sql                  import SparkSession, functions as F
from   delta.tables                 import DeltaTable
from   utils.market_pv              import market_currency, yahoo_pv, S3_save_extract
import pandas                       as pd
import requests, time, os, io


# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F

os.environ['NO_PROXY'] = '*'  #request 不用代理环境

today = date.today().strftime("%Y-%m-%d")
yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

save_bronze_parquet=S3_save_extract("bronze",format=None)
save_silver_parquet=S3_save_extract("silver",format=None)

@task
def cb_market_daily_raw():
    paras = yahoo_pv(start=yesterday, end=today,ticker='SPY', ticker_list=['SPY', '510050.SS'])
    df = paras.cb_market()
    save_bronze_parquet.save_daily(df,'cb_market_daily_raw')
    return df.shape[0]
    
@task
def cb_currency_daily_raw():
    paras = yahoo_pv(start=yesterday, end=today,ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X'])
    df = paras.cb_currency()
    save_bronze_parquet.save_daily( df,'cb_currency_daily_raw')
    return df.shape[0]
    
@task 
def market_daily_currency_callable():  
    S3=S3_save_extract("bronze", None)
    extract_cb_market_daily_raw      =S3.extract('cb_market_daily_raw')
    extract_cb_currency_daily_raw    =S3.extract('cb_currency_daily_raw')
    df                               =market_currency(extract_cb_market_daily_raw, extract_cb_currency_daily_raw)
    save_silver_parquet.save_daily(df,'market_daily_currency')
    return df.shape[0]

@task
def market_daily_upsert_history_currency():
    spark           = SparkSession.builder.appName("market_daily_upsert_history_currency").getOrCreate()
    today           = date.today().strftime("%Y-%m-%d")
    DAILY_PATH      = f"s3a://world-pool-bucket-version-1/silver/market/streaming/{today}_market_daily_currency.parquet"
    HIST_PATH       = "s3a://world-pool-bucket-version-1/silver/market/market_history_currency.parquet"
    
    market_daily_sp = spark.read.parquet(DAILY_PATH)
    
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (market_daily_sp.write.mode("overwrite").partitionBy("Date").parquet(HIST_PATH))

    return market_daily_sp.count()
     

with DAG(dag_id='market_daily',   schedule=None, start_date=datetime(2023, 1, 1),  catchup=False) as dag:
    start = EmptyOperator(task_id="yahoo_market")
    cb_market_daily_raw_task              = cb_market_daily_raw()
    cb_currency_daily_raw_task            = cb_currency_daily_raw()
    market_daily_currency_task            = market_daily_currency_callable()
    market_daily_upsert_history_currency  = market_daily_upsert_history_currency()
    end   = EmptyOperator(task_id="end")

    start >> [cb_market_daily_raw_task ,cb_currency_daily_raw_task ] >>market_daily_currency_task >> market_daily_upsert_history_currency >> end
