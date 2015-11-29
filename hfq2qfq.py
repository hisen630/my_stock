#!/usr/bin/env python2.7
#coding=utf-8

import logging
import sys

import argparse
import  pandas as pd
import lib.utils as utils
import conf.conf as conf


def _doqfq(code):
    hfqDF = pd.read_sql('select * from t_daily_hfq_stock where code="%s"'%code, utils.getEngine())

    factor = pd.read_sql('select * from t_daily_fqFactor where code="%s" and date="%s"'%(code, hfqDF.tail(1)['date'].values[0]), utils.getEngine())['factor'].values[0]

    print hfqDF.tail(1)
    print factor

def _hfq2qfq():

    codes = pd.read_sql('select code from t_stock_basics', utils.getEngine())
    if conf.DEBUG:
        codes = codes[:1]

    codes['code'].map(_doqfq)
    


if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')
    args = parser.parse_args()

    if args.production:
        conf.DEBUG = False

    _hfq2qfq()
