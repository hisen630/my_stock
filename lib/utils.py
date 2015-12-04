#!/usr/bin/env python2.7
#coding=utf-8

import time
import socket # for testing
from functools import wraps

import MySQLdb
import pandas as pd
from sqlalchemy import create_engine

import tushare as ts

import sys
sys.path.append('..')
import conf.conf as conf


def retry(MyException, tries=4, delay=3, backoff=2, logger=None):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except MyException as ex:
                    msg = '%s, Retrying in %d seconds...'%(str(ex), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry


@retry(Exception,logger=conf.logger)
def getStockBasics():
    return pd.read_sql('select * from %s'%conf.STOCK_BASIC_TABLE, con=getEngine(), index_col='code')


@retry(Exception,logger=conf.logger)
def ts_parse_fq_factor_wrap(code):
    try:
        df = ts.stock.trading._parase_fq_factor(code, '', '')
    except AttributeError as ex:
        # if a stock new to market, there's no fq factor
        # and sina returns a bad formatted of empty data, will cause:
        #  AttributeError: 'list' object has no attribute 'keys'
        return None
    return df


@retry(Exception,logger=conf.logger)
def ts_get_h_data_wrap(code,start, end, autype='hfq'):
    return ts.get_h_data(code, autype=autype, start=start, end=end, retry_count=5, pause=0.01)

@retry(Exception,logger=conf.logger)
def ts_get_realtime_quotes_wrap(code):
    return ts.get_realtime_quotes_wrap(code)

@retry(Exception,logger=conf.logger)
def downloadStockBasics():

    stockBasics = ts.get_stock_basics()

    stockBasics.sort_index(ascending=True, inplace=True)
    assert stockBasics.shape[0] > conf.STOCK_NUMBER_LOW_BOUND

    executeSQL("delete from %s"%conf.STOCK_BASIC_TABLE)
    stockBasics.to_sql(name=conf.STOCK_BASIC_TABLE, con=getEngine(), if_exists="append")

    return stockBasics


def getEngine(typeStr='mysql'):
    return create_engine('mysql://%s:%s@%s/%s?charset=%s'%(
        conf.mysqlConfig['user'],
        conf.mysqlConfig['passwd'],
        conf.mysqlConfig['host'],
        conf.DB_NAME,
        conf.mysqlConfig['charset']))


def getPeriodByYears(startDate, endDate, timeToMarket=None):

    if timeToMarket is not None:
        timeToMarket = str(timeToMarket)
        timeToMarket = '-'.join((timeToMarket[:4], timeToMarket[4:6], timeToMarket[6:]))
        if timeToMarket > startDate:
            startDate = timeToMarket

    _ranges = []
    _years = pd.period_range(startDate, endDate, freq='A')
    for idx, y in enumerate(_years):
        if idx == 0:
            _ranges.append((startDate, "%d-12-31"%y.year)) 
        elif idx == len(_years) - 1:
            _ranges.append(("%d-01-01"%y.year, endDate))
        else:
            _ranges.append(("%d-01-01"%y.year, "%d-12-31"%y.year))
    return _ranges


def getConn():
    conf.mysqlConfig["db"] = conf.DB_NAME
    try:
        conn = MySQLdb.connect(**conf.mysqlConfig)
    except Exception,e:
        # init the db
        if e.args[0] == 1049:
            conf.mysqlConfig.pop("db")
            conn = MySQLdb.connect(**conf.mysqlConfig)
        else:
            raise e
    return conn


def executeSQL(sql):
    '''
    this wrapper sucks. cannot get any data out, just execute a sql.
    '''
    conn = getConn()
    cursor = conn.cursor()
    conf.logger.info("Execute sql: %s"%sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


if '__main__' == __name__:

    print getPeriodByYears('2014-01-03','2015-11-23','20140202')
    print getStockBasics().head()
    @retry(Exception)
    def check():
        sk = socket.socket()
        sk.settimeout(1)
        sk.connect(('6.6.6.6', 80))

    check()
