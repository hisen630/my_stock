#!/usr/bin/env python2.7
#coding=utf-8

import sys

import argparse
import tushare as ts
import pandas as pd
import lib.utils as utils
import conf.conf as conf


def _doqfq(code):
    logging.info("qfq for %s."%code)
    hfqDF = pd.read_sql('select * from t_daily_hfq_stock where code="%s"'%code, utils.getEngine()).set_index('date').sort_index(ascending=False)
    if hfqDF is None or hfqDF.empty:
        return

    factors = pd.read_sql('select * from t_daily_fqFactor where code="%s" and date="%s"'%(code, hfqDF.head(1).index.values[0]), utils.getEngine())
    # off market
    if factors is None or factors.empty:
        return

    factor = factors['factor'].values[0]

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
        conf.logger.info("Saving %s qfq data."%code)
        hfqDF.to_sql(name=conf.STOCK_QFQ_TABLE, con=utils.getEngine(), if_exists='append')
        conf.logger.info("Saved %s qfq data."%code)
    
    if conf.DEV:
        conf.logger.debug(hfqDF.tail(1))
        conf.logger.debug(factor)
        conf.logger.debug(_rate)

def _hfq2qfq(beginCode=None):

    codes = pd.read_sql('select code from t_stock_basics', utils.getEngine())
    if conf.DEV:
        codes = codes[:1]

    if not conf.DEBUG and beginCode is None:
        utils.executeSQL('delete from t_daily_qfq_stock')

    if beginCode is not None:
        codes = codes[codes['code']>=beginCode]
    codes['code'].map(_doqfq)
    

if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--beginCode", type=str, help='''starts with this code''')
    args = parser.parse_args()

    _hfq2qfq(args.beginCode)
