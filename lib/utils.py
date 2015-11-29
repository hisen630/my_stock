#!/usr/bin/env python2.7
#coding=utf-8

import logging

import time

import sys
sys.path.append("..")
import conf.conf as conf
import mylog

import MySQLdb

import tushare as ts
from sqlalchemy import create_engine

def getEngine(typeStr='mysql'):
    return create_engine('mysql://%s:%s@%s/%s?charset=%s'%(
        conf.mysqlConfig['user'],
        conf.mysqlConfig['passwd'],
        conf.mysqlConfig['host'],
        conf.DB_NAME,
        conf.mysqlConfig['charset']))

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
    logging.info("Execute sql: %s"%sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def executemany(sql_template, args):
    '''
    this wrapper sucks. cannot get any data out, just execute a sql.
    '''
    conn = getConn()
    cursor = conn.cursor()
    logging.info("Execute sql: %s to save %d lines of data."%(sql_template, len(args)))
    cursor.executemany(sql_template, args)
    conn.commit()
    cursor.close()
    conn.close()


def downloadStockBasics():
    
    stockBasics = ts.get_stock_basics()
    #stockBasics.insert(0,"update_date",time.strftime( conf.ISO_DATE_FORMAT, time.localtime()),True)
    executeSQL("delete from t_stock_basics")
    stockBasics.to_sql(name="t_stock_basics",con=getEngine(),if_exists="append")
    return stockBasics

def splitDateRange(startDate, endDate,timeToMarket=None):
    """The (startDate, endDate) may span over a very large range,
        and this is not good for performance consideration if the downloader
        is based on the tusahre lib which is our primary downloader.
        And this method is called to split the range 
        into several smaller(year) range.
    """
    _ranges = []

    if timeToMarket is not None:
        timeToMarket = str(timeToMarket)
        timeToMarket = timeToMarket[:4]+"-"+timeToMarket[4:6]+"-"+timeToMarket[6:] 
        if timeToMarket > startDate:
            startDate = timeToMarket

    years = range(int(startDate.split('-')[0]), int(endDate.split('-')[0])+1)
    # only one year
    if len(years) == 1:
        _ranges.append((startDate, endDate))
        return _ranges
    
    for idx, y in enumerate(years):
        if idx == 0:
            _ranges.append((startDate, "%d-12-31"%y)) 
        elif idx == len(years) - 1:
            _ranges.append(("%d-01-01"%y, endDate))
        else:
            _ranges.append(("%d-01-01"%y, "%d-12-31"%y))
    return _ranges


# test codes
if "__main__" == __name__:
    downloadStockBasics()
