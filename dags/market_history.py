
from   airflow                      import DAG
from   airflow.operators.python     import PythonOperator
from   airflow.operators.empty      import EmptyOperator
from   airflow.decorators           import task
from   datetime                     import datetime,date, timedelta
from   pyspark.sql                  import SparkSession, functions as F
from   delta.tables                 import DeltaTable
from   utils.market_function        import yahoo_pv, S3_save_extract
from   delta                        import configure_spark_with_delta_pip

import logging
import pandas                       as pd
import requests, time, os, io

os.environ['NO_PROXY'] = '*'  #request 不用代理环境

save_bronze_parquet=S3_save_extract("bronze",format=None)
save_silver_parquet=S3_save_extract("silver",format=None)

S3=S3_save_extract("bronze", None)
S3_silver=S3_save_extract("silver",format=None)

@task
def cb_market_hist_raw():
    paras = yahoo_pv(start='2021-11-01', end='2022-01-01',ticker='510050.SS', ticker_list=['SPY', '510050.SS'])
    df    = paras.cb_market()
    save_bronze_parquet.save_hist(df,'cb_market_history_raw')
    return df.shape[0]
  
@task
def cb_currency_hist_raw():
    paras = yahoo_pv(start='2021-11-01', end='2022-01-01',ticker='CNY=X', ticker_list=['CNY=X', 'EURCHF=X'])
    df    = paras.cb_currency()
    save_bronze_parquet.save_hist(df,'cb_currency_history_raw')
    return df.shape[0]
  
@task
def market_hist_currency_save():
    extract_cb_market_history_raw       =S3.extract('cb_market_history_raw')
    extract_cb_currency_history_raw     =S3.extract('cb_currency_history_raw')
    market_hist_currency                =S3_silver.market_currency(extract_cb_market_history_raw,extract_cb_currency_history_raw)
    save_market_hist_currency           =S3_silver.save_hist(market_hist_currency, "market_hist_currency")  # 存完成的parquet 文件
    return save_market_hist_currency.shape[0]
    
@task
def market_hist_currency_partition():
    market_hist_currency                =S3_silver.extract('market_hist_currency')
    market_hist_currency_partition      =S3_silver.market_history_currency_partition(market_hist_currency,"market_hist_currency_partition")                                                                    
    return market_hist_currency_partition.shape[0]
                                                                               
with DAG(dag_id='market_history', schedule_interval=None, start_date=datetime(2023, 1, 1),  catchup=False) as dag:
    start = EmptyOperator(task_id="yahoo_market")
    cb_market_hist_raw_task             = cb_market_hist_raw()
    cb_currency_hist_raw_task           = cb_currency_hist_raw()
    market_hist_currency_save_task      = market_hist_currency_save()
    market_hist_currency_partition_task = market_hist_currency_partition()
    end   = EmptyOperator(task_id="end")

    # Set task dependencie
    start >> cb_currency_hist_raw_task >> cb_market_hist_raw_task >> market_hist_currency_save_task >> market_hist_currency_partition_task >> end
