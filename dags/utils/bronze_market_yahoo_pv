import pandas as pd
import requests, time, os
from   datetime import datetime
from   pyspark.sql import SparkSession, functions as F

class yahoo_pv:

    def __init__(self, start, end, ticker, ticker_list):
        self.history_start  = self.ts_df_s_d(start)
        self.history_end    = self.ts_df_s_d(end)
        self.ticker         = ticker
        self.ticker_list    = ticker_list
        self.spark          = SparkSession.builder.appName("myApp").getOrCreate() #避免调用下一个api 使用二次启动
        self.daily_start    = int((pd.to_datetime('today') - pd.Timedelta(days=1)).timestamp())
        self.daily_end      = int(pd.to_datetime('today').timestamp())

    #Fetche data of history
    @staticmethod
    def ts_df_s_d(date):
        return int(pd.to_datetime(date).timestamp())

    def history(self, start, end, ticker):  #把这些都是设置成活的变量
        start_type  = self.history_start if start  is None else start
        end_type    = self.history_end   if end    is None else end
        ticker_type = self.ticker        if ticker is None else ticker
        return start_type, end_type, ticker_type

    def daily(self, start, end, ticker):  #把这些都是设置成活的变量
        start_type  = self.daily_start   if start  is None else start
        end_type    = self.daily_end     if end    is None else end
        ticker_type = self.ticker        if ticker is None else ticker
        return start_type, end_type, ticker_type

    ################################################################################################ Partie 1 History ################################################################################################
    def fc_json(self, start=None, end=None, ticker=None):
        start_type, end_type, ticker_type = self.history(start, end, ticker)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_type}"
        params = {"period1": start_type, "period2": end_type, "interval": "1d"}     #interval放在里面避免输入
        return requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}).json()   #伪装成浏览器

    def ts_json_df(self, data=None,start=None, end=None, ticker=None):
        start_type, end_type, ticker_type = self.history(start, end, ticker)
        data = self.fc_json(start=start_type, end=end_type, ticker=ticker_type) if data is None else data  #分情况调用上面

        indicators = data['chart']['result'][0]['indicators']
        open       = indicators['quote'][0]['open']
        high       = indicators['quote'][0]['high']
        low        = indicators['quote'][0]['low']
        close      = indicators['quote'][0]['close']
        volume     = indicators['quote'][0]['volume']
        adjclose   = indicators['adjclose'][0]['adjclose']
        date_list  = data['chart']['result'][0]['timestamp']
        return pd.DataFrame({'date': date_list, 'ticker': ticker_type,  'open': open, 'high': high, 'low': low, 'close': close, 'volume': volume, 'adjclose': adjclose })

    def ts_combined_df(self, data=None, start=None, end=None, ticker=None):
        start_type, end_type, ticker_type = self.history(start, end, ticker)
        data = [self.ts_json_df(data=data, start=start, end=end, ticker=ticker_type) for ticker_type in self.ticker_list]
        return pd.concat(data,ignore_index=True).sort_values('date') #ignore_index=True 不要重新排列索引节省算力

    def ts_df_spk(self, start=None, end=None, ticker=None, data=None):
        return  self.spark.createDataFrame(self.ts_combined_df())

    def ts_s_d_spk(self):  #直接引用不加变量，时候不用使用None
        data=self.ts_df_spk()
        return data.withColumn("date", F.to_date(F.from_unixtime(F.col("date"), "yyyy-MM-dd")))

    ################################################################################################ Partie 2 Daily ################################################################################################

    def fc_json_daily(self, start=None, end=None, ticker=None):
        start_type, end_type, ticker_type = self.daily(start, end, ticker)
        return self.fc_json( start=start_type, end=end_type, ticker=ticker_type)

    def ts_js_df_daily(self, start=None, end=None, ticker=None, data=None):
        start_type, end_type, ticker_type = self.daily(start, end, ticker)
        data=self.fc_json_daily(start=start_type, end=end_type, ticker=ticker_type)
        return self.ts_json_df(data, start=start_type, end=end_type, ticker=ticker_type)

    def ts_combined_df_daily(self, start=None, end=None, ticker=None, data=None):
        dfs = [self.ts_js_df_daily(start=self.daily_start, end=self.daily_end, ticker=ticker_type) for ticker_type in self.ticker_list]
        return pd.concat(dfs,ignore_index=True).sort_values('date') #ignore_index=True 不要重新排列索引节省算力

    def ts_df_spk_daily(self):
        return  self.spark.createDataFrame(self.ts_combined_df_daily())

    def ts_s_d_spk_daily(self):  #直接引用不加变量，时候不用使用None
        data=self.ts_df_spk_daily()
        return data.withColumn("date", F.to_date(F.from_unixtime(F.col("date"), "yyyy-MM-dd")))
