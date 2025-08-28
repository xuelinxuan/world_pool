import requests, time, os, pytest
import yfinance    as yf
import pandas      as pd
from   datetime    import datetime
from   pyspark.sql import SparkSession, functions as F

class yahoo_pv:

    def __init__(self, start, end, ticker, ticker_list):
        self.start           = self.ts_df_s_d(start)
        self.end             = self.ts_df_s_d(end)
        self.ticker          = ticker
        self.ticker_list     = ticker_list
        self.spark           = SparkSession.builder.appName("myApp").getOrCreate() #避免调用下一个api 使用二次启动

    #Fetche data of history
    @staticmethod
    def ts_df_s_d(date):
        return int(pd.to_datetime(date).timestamp())
    ################################################################################################ Partie 1 currency ################################################################################################
    def fc_currency(self):
        data=yf.Ticker(self.ticker).history(start=self.start, end=self.end).assign(nation=self.ticker).reset_index().rename(columns={'Date':'date','Close':'currency'})
        return data.drop(columns=['Open','Low','High','Volume','Dividends','Stock Splits'])[['date', 'nation', 'currency']]

    def cb_currency(self):
        holder = self.ticker
        data = []
        for t in self.ticker_list:
            self.ticker = t
            data.append(self.fc_currency())
        self.ticker = holder
        return pd.concat(data, ignore_index=True).sort_values('date')

    def sp_currency(self):
        return self.spark.createDataFrame(self.cb_currency()).withColumn("date", F.to_date(F.col("date")))
    ################################################################################################ Partie 2 market ################################################################################################
    def fc_market(self):
        ticker = yf.Ticker(self.ticker)
        adj_close = ticker.history(start=self.start, end=self.end, auto_adjust=True)[["Close"]].rename(columns={"Close": "adj_close"})
        data=ticker.history(start=self.start, end=self.end).drop(columns=['Dividends','Stock Splits','Capital Gains']).rename(columns={"Open": "open","High": "high","Low": "low","Close": "close","Volume": "volume"})
        return data.join(adj_close).reset_index().rename(columns={'Date':'date'}).assign(ticker=self.ticker)[['date',	'ticker',	'open',	'high',	'low',	'close',	'volume',	'adj_close']]

    def cb_market(self):
        holder = self.ticker
        ticker = yf.Ticker(self.ticker)
        data = []
        for t in self.ticker_list:
            self.ticker = t
            data.append(self.fc_market())
        self.ticker = holder
        return pd.concat(data, ignore_index=True).sort_values('date')

    def sp_market(self):
        return self.spark.createDataFrame(self.cb_market()).withColumn("date", F.to_date(F.col("date")))
