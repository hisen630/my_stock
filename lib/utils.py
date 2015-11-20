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
    conn = getConn()
    cursor = conn.cursor()
    logging.info("Execute sql: %s"%sql)
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()

def executemany(sql_template, args):
    conn = getConn()
    cursor = conn.cursor()
    logging.info("Execute sql: %s to save %d lines of data."%(sql_template, len(args)))
    cursor.executemany(sql_template, args)
    conn.commit()
    cursor.close()
    conn.close()

def downloadStockBasics():
    
    stockBasics = ts.get_stock_basics()
    stockBasics.insert(0,"update_date",time.strftime( conf.ISO_DATE_FORMAT, time.localtime()),True)
    try:
        stockBasics.to_sql(name="t_stock_basics", flavor="mysql",con=getConn(),if_exists="append")
    except Exception,e:
        # duplicated key, do nothing
        if e[0]==1062:
            pass


# test codes
if "__main__" == __name__:
    downloadStockBasics()
