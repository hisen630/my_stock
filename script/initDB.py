#!/usr/bin/env python2.7
#coding=utf-8

"""
This script is just for generally initialize MySQL databases and tables.
"""

import argparse
import logging

import sys
sys.path.append("..")

import conf.conf as conf
import lib.utils as utils

def _createDB():
    sql = "CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci;"%conf.DB_NAME
    utils.executeSQL(sql)
    logging.info("Created database %s."%conf.DB_NAME)

tableSqlDict = {
        "dailyStock":'''
            create table `t_daily_stock` (
               `stock_code` varchar(16) NOT NULL COMMENT 'stock code',
               `deal_date` date NOT NULL COMMENT 'date',
               `open_price` float(10,2) NOT NULL COMMENT 'open price',
               `high_price` float(10,2) NOT NULL COMMENT 'highest price of the day',
               `close_price` float(10,2) NOT NULL COMMENT 'close price',
               `low_price` float(10,2) NOT NULL COMMENT 'lowest price of the day',
               `volume` bigint(20) NOT NULL COMMENT 'volume',
               `amount` bigint(20) NOT NULL COMMENT 'deal amount of money',
               UNIQUE KEY `idx_code_date` (`stock_code`, `deal_date`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;''',
            }
def _createTbl(tableName):
    assert tableName in tableSqlDict
    utils.executeSQL(tableSqlDict[tableName])


if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--create", type=str, 
            choices=["db_stock", "tbl_dailyStock", "tbl_stockBasic", "all"],
            help='''
Create the database or table.
all : create all the needed database and tables.
db_stock : create the database.
tbl_dailyStock : create the table of name "dailyStock", which records the daily stock info.
tbl_stockBasic : craete the table of name "stockBasic", which records the stock basic info.
            ''', required=True)
    parser.add_argument("-f", "--force", action="store_true",
            help="if this option is set, will do init even if the db/table exists, which leads to data deleted.")

    args = parser.parse_args()

    if args.create:
        if args.create.lower() in ("db_stock", "all"):
            _createDB()
        if args.create.lower() in ("tbl_dailystock", "all"):
            _createTbl("dailyStock")

