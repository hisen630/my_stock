#!/usr/bin/env python2.7
#coding=utf-8

import argparse

import pandas as pd
from pandas import DataFrame,Series

import tushare as ts

import lib.utils as utils
import conf.conf as conf

@utils.retry(Exception)
def ts_get_h_data_wrapper(code,start, end, autype='hfq'):
    return ts.get_h_data(code, autype=autype, start=start, end=end, retry_count=5, pause=0.01)

class HistoryDataDownloader(object):

    def __init__(self, startDate, endDate):
        self.stockBasics = utils.downloadStockBasics()
        self.startDate = startDate
        self.endDate = endDate

    def _downloadSingle(self, code):
        codeDF = DataFrame()
        for (start, end) in utils.getPeriodByYears(self.startDate, self.endDate, self.stockBasics.ix[code]['timeToMarket']):
            descStr = " (%s, %s, %s) "%(code, start, end)
            conf.logger.info("Downloading %s"%descStr)

            df = ts_get_h_data_wrapper(code, start, end)

            if df is None or df.shape[0] == 0:
                conf.logger.warning("No data for %s"%descStr)
                return
            conf.logger.info("Downloaded %s with shape %s."%(descStr, df.shape))
            codeDF = pd.concat([codeDF, df])

        codeDF.insert(0, "code", code, True)
        conf.logger.info("Saving %s with shape %s."%(descStr, codeDF.shape))
        codeDF.to_sql(name=conf.STOCK_HFQ_TABLE, con=utils.getEngine(), if_exists="append")
        conf.logger.info("Saved %s with shape %s."%(descStr, codeDF.shape))
            

    def download(self):
        codes = self.stockBasics.index
        if conf.DEV:
            codes = codes[:10]
        codes.map(self._downloadSingle)


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

    if args.production:
        conf.DEV = False

    downloader = HistoryDataDownloader(startDate=args.start, endDate=args.end)
    downloader.download()
