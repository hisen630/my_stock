#!/usr/bin/env python2.7
#coding=utf-8

import sys
import time
import argparse

from dateutil.parser import parse as dateparse

import pandas as pd
from pandas import DataFrame,Series

import tushare as ts

import lib.utils as utils
import conf.conf as conf

class DailyDataDownloader(object):

    def __init__(self):
        self.today = dateparse(time.asctime()).date()
        self.stockBasics = utils.downloadStockBasics()

    def _downloadSingle(self, code):
        descStr = " (%s, %s) "%(code, str(self.today))
        conf.logger.info("Downloading daily %s."%descStr)

        df = self._ts_get_realtime_quotes_wrap(code)

        if df is None or df.empty:
            conf.logger.warning("No daily data for %s."%descStr)
            return
        conf.logger.info("Downloaded daily data for %s with shape %s."%(descStr, df.shape))

        df = df[['code','open','high','pre_close','price','low','volume','amount','date']].set_index('date')
        df.rename(columns={'price':'close'}, inplace=True)

        # a brand new code into market, could also like this, the get_realtime_quotes may return something
        if ((float(df['high']) == 0) & (float(df['low'])==0)):
            conf.logger.warning("high&low are both 0 for %s."%descStr)
            return 

        fqdf = utils.ts_parse_fq_factor_wrap(code)
        fqdf.insert(0,'code',code,True)
        fqdf.drop_duplicates('date').set_index('date').sort_index(ascending=False)
        fqdf = fqdf.head(1)

        _rate = float(fqdf['factor']) / float(df['close'])

        fqdf = fqdf.drop('pre_close', axis=1)
        for label in ['open', 'high', 'close', 'low']:
            df[label] = float(df[label]) * _rate
            #df[label] = df[label].map(lambda x:'%.2f'%x)
            df[label] = df[label].astype(float)

        conf.logger.info("Saving daily fq factor for %s."%descStr)
        fqdf.to_sql(name=conf.STOCK_FQ_FACTOR, con=utils.getEngine(), if_exists='append', chunksize=20000)
        conf.logger.info("Saved daily fq factor for %s."%descStr)

        conf.logger.info("Saving daily hfq data for %s."%descStr)
        df.to_sql(name=conf.STOCK_HFQ_TABLE, conf=utils.getEngine(), if_exists='append', chunksize=20000)
        conf.logger.info("Saving daily hfq data for %s."%descStr)

    
if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    downloader = DailyDataDownloader()
    downloader.download()
    conf.logging.info("Download daily data successfully.")

