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
        "downloadTaskStatus":'''
            create table `t_download_task_status` (
               `id` int(16) NOT NULL AUTO_INCREMENT,
               `first_run_time` date NOT NULL COMMENT 'first run time.',
               `last_run_time` date NOT NULL COMMENT 'last run time.',
               `run_times` int(8) NOT NULL COMMENT 'how many time does this task ran.',
               `options` varchar(1024) NOT NULL COMMENT 'the task cmd options.',
               `last_success_code` varchar(16) NOT NULL COMMENT 'last success stock code.',
               `last_success_start` date NOT NULL COMMENT 'start date.',
               `last_success_end` date NOT NULL COMMENT 'end date.',
               `update_time` date NOT NULL COMMENT 'last touch this record.',
               `status` varchar(16) NOT NULL COMMENT 'indicate this task status.',
               PRIMARY KEY(`id`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        ''',
            }
def _createTbl(tableName):
    assert tableName in tableSqlDict
    utils.executeSQL(tableSqlDict[tableName])


if '__main__' == __name__:

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--create", type=str, 
            choices=["db_stock", "tbl_dailyStock", "tbl_stockBasic","tbl_downloadTaskStatus", "all"],
            help='''
Create the database or table.
all : create all the needed database and tables.
db_stock : create the database.
tbl_dailyStock : create the table of name "dailyStock", which records the daily stock info.
tbl_stockBasic : create the table of name "stockBasic", which records the stock basic info.
tbl_downloadTaskStatus : create the table of name "downloadTaskStatus", which records the download task's status, to provide the breakpoint.
            ''', required=True)
    parser.add_argument("-f", "--force", action="store_true",
            help="if this option is set, will do init even if the db/table exists, which leads to data deleted.")

    args = parser.parse_args()

    if args.create:
        if args.create.lower() in ("db_stock", "all"):
            _createDB()
        if args.create.lower() in ("tbl_dailystock", "all"):
            _createTbl("dailyStock")
        if args.create.lower() in ("tbl_downloadtaskstatus", "all"):
            _createTbl("downloadTaskStatus")

