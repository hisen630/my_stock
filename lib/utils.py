#!/usr/bin/env python2.7
#coding=utf-8

import logging

import conf.conf as conf
import mylog

import MySQLdb

def getConn():
    conn = MySQLdb.connect(**conf.mysqlConfig)
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
