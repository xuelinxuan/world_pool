import requests, time, os, pytest, boto3, io, sys, random
import yfinance            as yf
import pandas              as pd
from   delta.tables        import DeltaTable
from   datetime            import datetime, date, timedelta
from   pyspark.sql         import SparkSession, functions as F
from   delta               import configure_spark_with_delta_pip
from   requests.adapters   import HTTPAdapter
from   urllib3.util.retry  import Retry

class yahoo_pv:

    def __init__(self, start, end, ticker, ticker_list):
        self.start           = self.ts_df_s_d(start)
        self.end             = self.ts_df_s_d(end)
        self.ticker          = ticker
        self.ticker_list     = ticker_list
        self._spark          = None

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
        #避免限流
        time.sleep(60)
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

class S3_save_extract:
    _aws_access_key_id     = "AKIAWDYU6IA6HRAXK7XW"
    _aws_secret_access_key = "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"
    _region_name           = "us-east-1"
    _bucket                = "world-pool-bucket-version-1"

    def __init__(self, niveau, format):
        self.niveau   = niveau
        self.format   = format
        self.today    = date.today().strftime("%Y-%m-%d")
        self._spark    = None 
        self.s3       = boto3.client( "s3",aws_access_key_id    =self._aws_access_key_id,
                                     aws_secret_access_key      =self._aws_secret_access_key,
                                     region_name                =self._region_name)
        

        # os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar pyspark-shell"

        # builder = (SparkSession.builder
        #       # —— 你已有的 S3A & Delta 配置原样保留 ——
        #       .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        #       .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        #       .config("spark.hadoop.fs.s3a.access.key", self._aws_access_key_id)
        #       .config("spark.hadoop.fs.s3a.secret.key", self._aws_secret_access_key)
        #       #时间秒问题
        #       .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        #       .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        #       .config("spark.hadoop.fs.s3a.connection.maximum-lifetime", "86400000")
        #       .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        #       .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")

        #       .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        #       #dalta
        #       .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        #       .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        #       .config("spark.delta.logStore.class","org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
        #     )
        # self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    def _get_spark(self):
        if self._spark is None:
            # 现在 jars 已在 /opt/spark/jars 下，无需再设 PYSPARK_SUBMIT_ARGS
            builder = (
                SparkSession.builder
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                .config("spark.hadoop.fs.s3a.access.key", self._aws_access_key_id)
                .config("spark.hadoop.fs.s3a.secret.key", self._aws_secret_access_key)
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                # 超时等
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
                .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
                .config("spark.hadoop.fs.s3a.connection.maximum-lifetime", "86400000")
                .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
                .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
                # Delta
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
            )
            self._spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return self._spark

    # def stop_spark(self):
    #     if self._spark is not None:
    #         self._spark.stop()
    #         self._spark = None
            
    def save_hist(self, df, filename):
        buffer = io.BytesIO()  #Body=buffer.getvalue() 其实就是把你写到 内存缓冲区（BytesIO）
        df.to_parquet(buffer, index=True)
        self.s3.put_object(Bucket=self._bucket, Key=f"{self.niveau}/market/{filename}.parquet", Body=buffer.getvalue())
        return df

    def save_daily(self, sp, filename):
        key = f"{self.niveau}/market/streaming/{self.today}_{filename}.parquet"
        return sp.write.mode("overwrite").parquet(f"s3a://{self._bucket}/{key}")

    def extract(self, filename):
        obj = self.s3.get_object(Bucket = "world-pool-bucket-version-1", Key= f"{self.niveau}/market/{filename}.parquet")
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        return df

    # ---------- 不用 Spark 的方法保持不变 ----------

    def market_currency(self, df,serie):
        spark = self._get_spark()
        #outer join
        outer_join=df.join(serie, how="outer")  #按照双索引join：Date 和 Nation
        #sort+fill
        sort=outer_join.sort_index(level=["Nation", "Date"])
        holder = sort["ticker"] #seire 避免机构有index
        sort_col=["Open","High","Low","Close","Adj_close","Volume"]
        #df/serie
        sort[sort_col]=sort[sort_col].div(sort["currency"], axis=0)
        sort["ticker"]=holder #放回来
        #转为ms 方便spark 使用
        sort.index = sort.index.set_levels(sort.index.levels[0], level=0) 
        data    = sort.reset_index().sort_values("Date")
        # data_sp = spark.createDataFrame(data)
        # data_sp = data_sp.withColumn("Date", F.to_date("Date")) #日期里类型
        # data_sp.withColumn("dt",   F.col("Date"))     #字符串类型，必秒java 要求ms 
        return data
        
    def market_history_currency_partition(self, df, filename):
        data_sp = spark.createDataFrame(df)
        data_sp = data_sp.withColumn("Date", F.to_date("Date")) #日期里类型
        data_sp.withColumn("dt",   F.col("Date"))     #字符串类型，必秒java 要求ms 
        path = f"s3a://{self._bucket}/{self.niveau}/market/{filename}"
        writer = (data_sp.write.format("delta").mode("overwrite").option("compression", "snappy"))
        return writer.partitionBy(*["dt"]).save(path)

    def merge_daily_hist(self):
        DAILY_PATH = f"s3a://world-pool-bucket-version-1/{self.niveau}/market/streaming/{self.today}_market_daily_currency.parquet"
        HIST_PATH  = "s3a://world-pool-bucket-version-1/{self.niveau}/market/market_daily_currency/"
        daily= self.spark.read.parquet(DAILY_PATH)
        DeltaTable.forPath(self.spark, HIST_PATH).alias("L") \
        .merge(daily.alias("D"), "L.Date = D.Date AND L.ticker = D.ticker") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        return None
