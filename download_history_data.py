#!/usr/bin/env python2.7
#coding=utf-8

import argparse

import pandas as pd
from pandas import DataFrame,Series

import tushare as ts

import lib.utils as utils
import conf.conf as conf


class HistoryDataDownloader(object):

    def __init__(self, startDate, endDate, breakpoint=None):
        self.breakpoint=breakpoint
        if self.breakpoint is not None:
            self.stockBasics = utils.getStockBasics()
        else:
            self.stockBasics = utils.downloadStockBasics()
        self.startDate = startDate
        self.endDate = endDate

    def _downloadSingle(self, code):
        codeDF = DataFrame()
        for (start, end) in utils.getPeriodByYears(self.startDate, self.endDate, self.stockBasics.ix[code]['timeToMarket']):
            descStr = " (%s, %s, %s) "%(code, start, end)
            conf.logger.info("Downloading %s"%descStr)

            df = utils.ts_get_h_data_wrap(code, start, end)

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
        if self.breakpoint is not None:
            codes = codes[codes>self.breakpoint]
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
    parser.add_argument("-g", "--greater", type=str, help='''
    Given a code, will download all the codes greater than the given one.
    ''')

    args = parser.parse_args()

    downloader = HistoryDataDownloader(startDate=args.start, endDate=args.end, breakpoint=args.greater)
    downloader.download()
