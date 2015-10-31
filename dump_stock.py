#!/usr/bin/env python2.7
#coding=utf-8

import logging
import sys
import time
import argparse

import tushare as ts

import lib.mylog
import lib.utils as utils
import conf.conf as conf


class StockDownload(object):
    """To describe how to download a stock's info
    """
    def __init__(self, code, startDate, endDate, fields=None):
        """
        code -- the stock code, a string
        startDate -- from which day to download, a string of format '%Y-%m-%d'
        endDate -- till which day to download(include), a string of format '%Y-%m-%d'
        fields -- which fields of the stock needed to download, a list if figured. But now is just None.
        """
        self.code = code
        self.startDate = startDate
        self.endDate = endDate
        self.fields = fields

        self.downloadRanges = self.__splitDateRange()
        logging.debug("Download _ranges of %s :%s"%(self.code,self.downloadRanges))

    def __splitDateRange(self):
        """The (self.startDate, self.endDate) may span over a very large range,
            and this is not good for performance consideration if the downloader
            is based on the tusahre lib which is our primary downloader.
            And this method is called in the __init__ method to split the range 
            into several smaller(year) range.
        """
        _ranges = []
        years = range(int(self.startDate.split('-')[0]), int(self.endDate.split('-')[0])+1)
        # only one year
        if len(years) == 1:
            _ranges.append((self.startDate, self.endDate))
            return _ranges
        
        for idx, y in enumerate(years):
            if idx == 0:
                _ranges.append((self.startDate, "%d-12-31"%y)) 
            elif idx == len(years) - 1:
                _ranges.append(("%d-01-01"%y, self.endDate))
            else:
                _ranges.append(("%d-01-01"%y, "%d-12-31"%y))
        return _ranges


class StockDownloader(object):
    """A powerful downloader based on tushare.
    """

    def __init__(self, stockList):
        self.stockList = stockList # a list of StockDownload instances
        self.saveMode = conf.saveMode # how to save the data

    def download(self):
        for stock in self.stockList:
            for (start, end) in stock.downloadRanges:
                descStr = " (%s, %s, %s) "%(stock.code, start, end)
                logging.info("Downloading %s."%descStr)

                df = ts.get_h_data(stock.code, start=start, end=end, pause=3)

                if df is None:
                    logging.info("No data for %s."%descStr)
                else:
                    logging.info("Downloaded %s with shape: %s"%(descStr, df.shape))
                logging.info("Saving %s."%descStr)
                self._save(stock.code, df)
                logging.info("Saved %s."%descStr)

    def _save(self, code, df):
        """Save the downloaded data, called automatically by the download method
        """
        if df is None:
            return

        if self.saveMode == 'mysql':
            #df.to_sql("t_daily_stock",utils.getConn(), flavor="mysql", if_exists="replace", index=True)
            sqlTemplate = '''
REPLACE INTO t_daily_stock(stock_code, deal_date, open_price, high_price, close_price, low_price, volume, amount) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            dataArgs = []
            for idx, row in df.iterrows():
                dataArgs.append((code, idx, row['open'], 
                    row['high'], row['close'], row['low'],
                    row['volume'], row['amount']))
            utils.executemany(sqlTemplate, dataArgs)

if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--code", type=str, help='''
    The stock code, set to "all" if all the stock data needed. You can set a list of code by separated by comma.
    ''', required=True)
    parser.add_argument("-g", "--greater", action="store_true", help='''
    if this option is set, all the codes greater than the code will be
        downloaded.
    ''')
    parser.add_argument("-s", "--start", type=str, help='''
    From which date(YYYY-MM-DD) to download. If not set, will using stock's timeToMarket.
    ''')
    parser.add_argument("-e", "--end", type=str, help='''
    Till which date(YYYY-MM-DD) to download. If not set, will using current date.
    ''')
    args = parser.parse_args()

    stockBasics = ts.get_stock_basics()
    stockList = []

    codeList = None
    if args.code.find(",")!=-1:
        # a list of codes
        codeList = args.code.split(",")
        logging.info("CodeList: Specified with size %d : %s."%(len(codeList), codeList))
    elif args.code.lower() == "all":
        codeList = list(stockBasics.index)
        logging.info("CodeList:ALL with size %d"%len(codeList))
    else:
        if args.greater:
            codeList = filter(lambda x:x>=args.code,list(stockBasics.index))
            logging.info("CodeList: Since %s with size %d."%(args.code,
                len(codeList)))
        else:
            codeList = [args.code,]
            logging.info("CodeList: %s"%codeList)

    for code in codeList:
        dateStr = args.start
        if args.start is None:
            dateStr = str(stockBasics.ix[code]['timeToMarket'])
            dateStr = dateStr[:4]+"-"+dateStr[4:6]+"-"+dateStr[6:]
        sd = StockDownload(code, 
                dateStr,
                time.strftime("%Y-%m-%d", time.localtime(time.time())) if args.end is None else args.end)
        stockList.append(sd)

    downloader = StockDownloader(stockList)
    downloader.download()
