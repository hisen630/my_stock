#!/usr/bin/env python2.7
#coding=utf-8

import logging
import sys
import time
import argparse

import pandas as pd
from pandas import DataFrame,Series

import tushare as ts

import lib.mylog
import lib.utils as utils
import conf.conf as conf

class DailyDataDownloader(object):

    def __init__(self,date, interval=10, retryTimes=5):
        self.date = date
        self.interval = interval if not conf.DEBUG else 1
        self.retryTimes = retryTimes
        self.stockBasics = utils.downloadStockBasics()

    def download(self):
        codes = self.stockBasics.index.values
        fqFactorDF = DataFrame()
        codeDF = DataFrame()

        for code in codes:
            descStr = " (%s, %s) "%(code, self.date)

            _intervalFactor = 2
            _interval = self.interval
            _retryCount = 0
            while _retryCount < self.retryTimes:
                _retryCount += 1
                logging.info("Downloading daily %s trying %d times."%(descStr, _retryCount))
                _interval *= _intervalFactor
                try:
                    _fqDF = ts.stock.trading._parase_fq_factor(code,'','')
                    _fqDF.insert(0,"code",code,True)
                    _fqDF = _fqDF.drop_duplicates('date').set_index('date').sort_index(ascending=False)
                    #_fqDF = _fqDF.ix[self.date]
                    _fqDF = _fqDF.head(1)

                    _df = ts.get_realtime_quotes(code)
                    _df = _df[['code','open','high','price','low','volume','amount','date']].set_index('date')
                    _df.rename(columns={'price':'close'},inplace=True)
                    _rate = float(_fqDF['factor'])/float(_df['close'])
                    for label in ['open', 'high', 'close', 'low']:
                        _df[label] = float(_df[label]) * _rate
                        #_df[label] = _df[label].map(lambda x:'%.2f'%x)
                        _df[label] = _df[label].astype(float)
                except Exception, e:
                    if _retryCount + 1 == self.retryTimes or conf.DEBUG:
                        raise e
                    logging.info("Download error, waiting for %d secs."%_interval)
                    time.sleep(_interval)
                    continue
                fqFactorDF = pd.concat([fqFactorDF,_fqDF])
                codeDF = pd.concat([codeDF, _df])
                break

            if conf.DEBUG:
                break

        self._save(fqFactorDF, codeDF)

    def _save(self, fqFactorDF, codeDF):
        logging.info("Saving daily fq factor.")
        fqFactorDF.to_sql(name='t_daily_fqFactor', con=utils.getEngine(), if_exists='append', chunksize=20000)
        logging.info("Saved daily fq factor.")
        logging.info("Saving daily hfq data.")
        codeDF.to_sql(name='t_daily_hfq_stock', con=utils.getEngine(), if_exists='append')
        logging.info("Saved daily hfq data.")

if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')
    args = parser.parse_args()

    if args.production:
        conf.DEBUG = False

    downloader = DailyDataDownloader('')
    downloader.download()

