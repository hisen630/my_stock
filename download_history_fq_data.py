#!/usr/bin/env python2.7
#coding=utf-8

import argparse
import pandas as pd
from pandas import DataFrame

from lib import utils
import conf.conf as conf

import tushare as ts

@utils.retry(Exception,logger=conf.logger)
def _ts_parse_fq_factor(code):
    try:
        df = ts.stock.trading._parase_fq_factor(code, '', '')
    except AttributeError as ex:
        # if a stock new to market, there's no fq factor
        # and sina returns a bad formatted of empty data, will cause:
        #  AttributeError: 'list' object has no attribute 'keys'
        return None
    return df

def _downloadSingle(code):
    conf.logger.info("Downloading %s fq factor."%code)
    df = _ts_parse_fq_factor(code)
    if df is None:
        conf.logger.warning("No fq factor for %s."%code)
        return
    df = df.drop_duplicates('date').set_index('date')
    df.insert(0,"code",code,True)
    conf.logger.info("Downloaded %s fq factor."%code)

    conf.logger.info("Saveing %s fq factor."%code)
    df.to_sql(name=conf.STOCK_FQ_FACTOR, con=utils.getEngine(), if_exists="append", chunksize=20000)
    conf.logger.info("Saved %s fq factor."%code)

def _downloadFqFactor(codes):
    codes.map(_downloadSingle)

if '__main__' == __name__:
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')
    parser.add_argument("-g", "--greater", type=str, help='''
    Given a code, will download all the codes greater than the given one.
    ''')


    args = parser.parse_args()

    stockBasics = utils.getStockBasics()
    if args.greater is not None:
        stockBasics = stockBasics[stockBasics.index>args.greater]
    if conf.DEV:
        stockBasics = stockBasics[:10]
    _downloadFqFactor(stockBasics.index)
    conf.logger.info("Download fq data successfully.")

