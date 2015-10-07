#!/usr/bin/env python2.7
#coding=utf-8

import logging

import conf.conf as conf
import mylog

import MySQLdb

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
