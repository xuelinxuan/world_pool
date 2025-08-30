import requests, time, os, pytest, boto3, io
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

class S3_save_extract:
    _aws_access_key_id     = "AKIAWDYU6IA6HRAXK7XW"
    _aws_secret_access_key = "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"
    _region_name           = "us-east-1"
    _bucket                = "world-pool-bucket-version-1"

    def __init__(self, niveau, format):
        self.niveau   = niveau
        self.format   = format
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            region_name=self._region_name
        )

    def save(self, df):
        holder = io.BytesIO()
        df.to_parquet(holder, engine="pyarrow", index=True, compression=self.format)
        holder.seek(0)

        key = f"{self.niveau}/market/{df}.parquet"
        self.s3.upload_fileobj(holder, self._bucket, key)
        print(f"✅ Upload ok! s3://{self._bucket}/{key}")
        return df
        
    def save(self, file_name):
        holder = io.BytesIO()
        file_name.to_parquet(holder, engine="pyarrow", index=True, compression=self.format)
        holder.seek(0)

        key = f"{self.niveau}/market/{df}.parquet"
        self.s3.upload_fileobj(holder, self._bucket, key)
        print(f"✅ Upload ok! s3://{self._bucket}/{key}")
        return df
        
    def (self, file_name):
        obj = self.s3.get_object(Bucket = "world-pool-bucket-version-1", Key= f"bronze/market/{file_name}")
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        print(f"✅ Loaded {file_name}, shape={df.shape}")
        return df



