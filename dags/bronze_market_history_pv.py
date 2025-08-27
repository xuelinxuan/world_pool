

from   airflow                      import DAG
from   airflow.operators.python     import PythonOperator
from   airflow.operators.empty      import EmptyOperator
from   datetime                     import datetime
from   pyspark.sql                  import SparkSession, functions as F
from   delta.tables                 import DeltaTable
from   utils.bronze_market_yahoo_pv import yahoo_pv
import requests, time, os
import pandas as pd

# from pyspark.sql import SparkSession
# import pyspark.sql.functions as F

os.environ['NO_PROXY'] = '*'  #request 不用代理环境

paras= yahoo_pv(start='2020-01-01', end='2022-01-01', ticker='SPY', ticker_list = ['SPY', '510050.SS'])


def market_history(**kwargs):
    paras= yahoo_pv(start='2020-01-01', end='2022-01-01', ticker='SPY', ticker_list = ['SPY', '510050.SS'])
    ts_s_d_spk=paras.ts_s_d_spk()
    ti=kwargs['ti']  #kwargs['ti'] 固定实例化方法
    ti.xcom_push(key='market_history_sp', value=ts_s_d_spk())

with DAG(
    dag_id='bronze_market',             # DAG 名称（在 Airflow UI 里显示）
    schedule_interval=None,  #手动  #'*/5 * * * *',  每 5 分钟运行一次  # 每天跑一次'@daily',   
    start_date=datetime(2023, 1, 1),        # DAG 生效开始日期
    catchup=False                           # 不补跑历史数据
) as dag:

    start = EmptyOperator(task_id='start')

    market_history_task= PythonOperator(
        task_id='market_history',
        python_callable=market_history,
        provide_context=True )

    placeholder_daily = EmptyOperator(task_id='daily_placeholder')
    placeholder_exchange_rate=EmptyOperator(task_id='exchange_rate_placeholder')


    # Set task dependencies
    start >> [market_history_task, placeholder_daily,exchange_rate_placeholder]
