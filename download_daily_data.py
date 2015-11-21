#!/usr/bin/env python2.7
#coding=utf-8

import logging
import argparse
import pandas as pd
from pandas import DataFrame

from lib import utils
import lib.mylog
import conf.conf as conf

import tushare as ts

def _downloadFqFactor(codes):
    factorDF = DataFrame()
    for code in codes:
        logging.info("Downloading %s fq factor."%code)
        df = ts.stock.trading._parase_fq_factor(code,'','')
        df.insert(0,"code",code,True)
        df = df.drop_duplicates('date').set_index('date')
        factorDF = pd.concat([factorDF, df])
        if conf.DEBUG:
            break

    logging.info("Deleting fq factor.")
    utils.executeSQL("delete from t_daily_fqFactor")
    logging.info("Saving fq factor.")
    factorDF.to_sql(name='t_daily_fqFactor',con=utils.getEngine(), if_exists="append")
    logging.info("Saved fq factor.")

class DailyDataDownloader(object):

    def __init__(self, interval=10, retryTimes=5):
        self.interval = interval if not conf.DEBUG else 1
        self.retryTimes = retryTimes
        self.stockBasics = utils.downloadStockBasics()

    def download(self):
        _downloadFqFactor(self.stockBasics.index.values)

if '__main__' == __name__:
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')

    args = parser.parse_args()

    if args.production:
        conf.DEBUG = False

    downloader = DailyDataDownloader()
    downloader.download()
