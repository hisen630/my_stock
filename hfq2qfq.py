#!/usr/bin/env python2.7
#coding=utf-8

import logging
import sys

import argparse
import tushare as ts
import  pandas as pd
import lib.utils as utils
import conf.conf as conf


def _doqfq(code):
    logging.info("qfq for %s."%code)
    hfqDF = pd.read_sql('select * from t_daily_hfq_stock where code="%s"'%code, utils.getEngine()).set_index('date').sort_index(ascending=False)

    factor = pd.read_sql('select * from t_daily_fqFactor where code="%s" and date="%s"'%(code, hfqDF.head(1).index.values[0]), utils.getEngine())['factor'].values[0]


    rt = ts.get_realtime_quotes(code)
    if rt is None:
        return
    if ((float(rt['high']) == 0) & (float(rt['low'])==0)):
        preClose = float(rt['pre_close'])
    else:
        preClose = float(rt['price'])

    _rate = factor / preClose

    for label in ['open','high','low','close']:
        hfqDF[label] /= _rate
        hfqDF[label] = hfqDF[label].map(lambda x:'%.2f'%x)
        hfqDF[label] = hfqDF[label].astype(float)

    if not conf.DEBUG:
        logging.info("Saving %s qfq data."%code)
        hfqDF.to_sql(name='t_daily_qfq_stock', con=utils.getEngine(), if_exists='append')
        logging.info("Saved %s qfq data."%code)
    
    if conf.DEBUG:
        print hfqDF.tail(1)
        print factor
        print _rate

def _hfq2qfq():

    codes = pd.read_sql('select code from t_stock_basics', utils.getEngine())
    if conf.DEBUG:
        codes = codes[:1]

    if not conf.DEBUG:
        utils.executeSQL('delete from t_daily_qfq_stock')

    codes['code'].map(_doqfq)
    

if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--production", action="store_true", help='''defalt is in debug mode, which only plays a little''')
    args = parser.parse_args()

    if args.production:
        conf.DEBUG = False

    _hfq2qfq()
