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

class HistoryDataDownloader(object):

    def __init__(self, startDate, endDate, interval=10, retryTimes=5):
        self.stockBasics = utils.downloadStockBasics()
        self.startDate = startDate
        self.endDate = endDate
        self.interval = interval if not conf.DEBUG else 1
        self.retryTimes = retryTimes

    def download(self):
        codes = self.stockBasics.index.values
        for code in codes:
            codeDF = DataFrame()
            for (start, end) in utils.splitDateRange(self.startDate, self.endDate,self.stockBasics.ix[code]['timeToMarket']):
                descStr = " (%s, %s, %s) "%(code, start, end)

                _intervalFactor = 2
                _interval = self.interval
                _retryCount = 0
                while _retryCount < self.retryTimes:
                    _retryCount += 1
                    logging.info("Downloading %s trying %d times."%(descStr, _retryCount))
                    _interval *= _intervalFactor
                    try:
                        df = ts.get_h_data(code,autype='hfq', start=start, end=end, retry_count=5, pause=0.01)
                    except Exception, e:
                        if _retryCount + 1 == self.retryTimes:
                            raise e
                        logging.info("Download error, waiting for %d secs."%_interval)
                        time.sleep(_interval)
                        continue
                    break

                if df is None:
                    logging.info("No data for %s."%descStr)
                else:
                    logging.info("Downloaded %s with shape: %s"%(descStr, df.shape))
                    codeDF = pd.concat([codeDF, df])

            self._save(code, codeDF,descStr)
            
            if conf.DEBUG:
                break

    def _save(self, code, codeDF,descStr):
        codeDF.insert(0, "code", code, True)
        logging.info("Saving %s."%descStr)
        codeDF.to_sql(name="t_daily_hfq_stock",con=utils.getEngine(), if_exists="append")
        logging.info("Saved %s with shape:%s."%(descStr,codeDF.shape))



if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str, help='''
    From which date(YYYY-MM-DD) to download. If not set, will using stock's timeToMarket.
    ''')
    parser.add_argument("-e", "--end", type=str, help='''
    Till which date(YYYY-MM-DD) to download. If not set, will using current date.
    ''')
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')

    args = parser.parse_args()

    downloader = HistoryDataDownloader(startDate=args.start, endDate=args.end)
    downloader.download()
