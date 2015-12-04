#!/usr/bin/env python2.7
#coding=utf-8

import argparse
import pandas as pd
from pandas import DataFrame

from lib import utils
import conf.conf as conf

import tushare as ts

@utils.retry(Exception)
def _ts_parse_fq_factor(code):
    return ts.stock.trading._parase_fq_factor(code, '', '')

def _downloadSingle(code):
    conf.logger.info("Downloading %s fq factor."%code)
    df = _ts_parse_fq_factor(code)
    df = df.drop_duplicates('date').set_index('date')
    df.insert(0,"code",code,True)
    conf.logger.info("Downloaded %s fq factor."%code)

    conf.logger.info("Saveing %s fq factor."%code)
    df.to_sql(name=conf.STOCK_FQ_FACTOR, con=utils.getEngine(), if_exists="append", chunksize=20000)
    conf.logger.info("Saved %s fq factor."%code)

def _downloadFqFactor(codes):
    if conf.DEV:
        codes = codes[:10]
    codes.map(_downloadSingle)

if '__main__' == __name__:
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')

    args = parser.parse_args()

    stockBasics = utils.getStockBasics()
    _downloadFqFactor(stockBasics.index)

