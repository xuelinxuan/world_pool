import requests, time, os, pytest, boto3
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
        #选列+增加
        data=yf.Ticker(self.ticker).history(start=self.start, end=self.end)[['Close']].assign(nation=self.ticker)
        #重命名
        data=data.rename(columns={'Close':'currency'})
        #为了统一时区，格式转换：1.变成str,2.去小时，3.变回datetime
        idx_str = data.index.astype(str).str.slice(0, 10) 
        data.index = pd.to_datetime(idx_str)
        return  data      

    def cb_currency(self):
        holder = self.ticker
        data = []
        for t in self.ticker_list:
            self.ticker = t
            data.append(self.fc_currency())
        self.ticker = holder
        data=pd.concat(data, ignore_index=False)
        # 修改格式
        mapping = {"CNY=X": "CN","EURCHF=X": "US"}
        data['nation']=data['nation'].replace(mapping)
        data.loc[data["nation"] == "US", "currency"] = 1
        rename=data.rename(columns={'nation':'Nation'})
        return  rename.set_index("Nation", append=True)
    ################################################################################################ Partie 2 market ################################################################################################
    def fc_market(self):
        #获取单独adjclose,新规则：auto_adjust=True
        adj_close = yf.Ticker(self.ticker).history(start=self.start, end=self.end, auto_adjust=True)[["Close"]].rename(columns={"Close": "Adj_close"})
        #原数据选列+增加ticker列
        data_origin=yf.Ticker(self.ticker).history(start=self.start, end=self.end)[['Open','High','Low','Close','Volume']].assign(ticker=self.ticker)
        #合并新数据
        data=data_origin.join(adj_close)
        idx_str = data.index.astype(str).str.slice(0, 10) 
        data.index = pd.to_datetime(idx_str)
        return  data
        

    def cb_market(self):
        holder = self.ticker
        ticker = yf.Ticker(self.ticker)
        data = []
        for t in self.ticker_list:
            self.ticker = t
            data.append(self.fc_market())
        self.ticker = holder
        mapping={'SPY':'US','510050.SS':'CN'}
        data=pd.concat(data, ignore_index=False)
        data['Nation']=data['ticker'].map(mapping)
        data=data.set_index('Nation',append=True)
        return data.reorder_levels(['Date','Nation']).sort_index()
        
def data_clean(df,serie):
    #outer join
    outer_join=df.join(serie, how="outer")
    #sort+fill
    sort=outer_join.sort_index(level=["Nation", "Date"]).ffill()
    holder = sort["ticker"] #seire 避免机构有index 
    sort_col=["Open","High","Low","Close","Adj_close","Volume"]
    #df/serie
    sort[sort_col]=sort[sort_col].div(sort["currency"], axis=0)
    sort["ticker"]=holder
    #format index + drop + sort
    data=sort.reset_index().set_index(['Date','ticker']).sort_index(level='Date',ascending=True)
    return data


