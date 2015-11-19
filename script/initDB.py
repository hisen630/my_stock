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
        "fqFactor":'''
            create table `t_daily_fqFactor` (
                `stock_code` varchar(16) NOT NULL COMMENT 'stock code',
                `date` date NOT NULL COMMENT 'date',
                `factor` float(32) NOT NULL COMMENT 'for calculating fq price',
                UNIQUE KEY `idx_code_date` (`stock_code`, `date`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        ''',
        "dailyHfqStock":'''
            create table `t_daily_hfq_stock` (
               `stock_code` varchar(16) NOT NULL COMMENT 'stock code',
               `deal_date` date NOT NULL COMMENT 'date',
               `open_price` float(10,4) NOT NULL COMMENT 'open price, hfq',
               `high_price` float(10,4) NOT NULL COMMENT 'highest price of the day, hfq',
               `close_price` float(10,4) NOT NULL COMMENT 'close price, hfq',
               `low_price` float(10,4) NOT NULL COMMENT 'lowest price of the day, hfq',
               `volume` bigint(20) NOT NULL COMMENT 'volume',
               `amount` bigint(20) NOT NULL COMMENT 'deal amount of money',
               UNIQUE KEY `idx_code_date` (`stock_code`, `deal_date`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;''',
        "dailyQfqStock":'''
            create table `t_daily_qfq_stock` (
                `stock_code` varchar(16) NOT NULL COMMENT 'stock code',
                `deal_date` date NOT NULL COMMENT 'date',
                `open_price` float(10,4) NOT NULL COMMENT 'open price, qfq',
                `high_price` float(10,4) NOT NULL COMMENT 'highest price of the day, qfq',
                `close_price` float(10,4) NOT NULL COMMENT 'close price, qfq',
                `low_price` float(10,4) NOT NULL COMMENT 'lowest price of the day, qfq',
                `volume` bigint(20) NOT NULL COMMENT 'volume',
                `amount` bigint(20) NOT NULL COMMENT 'deal amount of money',
                UNIQUE KEY `idx_code_date` (`stock_code`, `deal_date`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        ''',
        "stockBasic":'''
            create table `t_stock_basics` (
                `update_date` date NOT NULL COMMENT 'update date',
                `stock_code` varchar(16) NOT NULL COMMENT 'stock code',
                `name` varchar(32) NOT NULL COMMENT 'cn name',
                `industry` varchar(32) NOT NULL COMMENT '',
                `area` varchar(32) NOT NULL COMMENT '',
                `pe` float(10,4) NOT NULL COMMENT '',
                `outstanding` float(10, 4) NOT NULL COMMENT '',
                `totals` float(10,4) NOT NULL COMMENT '',
                `totalAssets` float(20,4) NOT NULL COMMENT '',
                `liquidAssets` float(20,4) NOT NULL COMMENT '',
                `fixedAssets` float(20,4) NOT NULL COMMENT '',
                `reserved` float(20,4) NOT NULL COMMENT '',
                `reservedPerShare` float(20,4) NOT NULL COMMENT '',
                `esp` float(10,4) NOT NULL COMMENT '',
                `bvps` float(10,4) NOT NULL COMMENT '',
                `pb` float(10,4) NOT NULL COMMENT '',
                `timeToMarket` int(10) NOT NULL COMMENT '',
                UNIQUE KEY `idx_code_date` (`stock_code`, `update_date`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        ''',
        "sandboxStatus":'''
            create table `t_sandbox_status` (
                `key` varchar(128) NOT NULL COMMENT '',
                `start_time` date NOT NULL COMMENT '',
                `update_time` date NOT NULL COMMENT '',
                `status` int NOT NULL DEFAULT 0 COMMENT '1:HIST_ING, 2:HIST_DONE, 3:DAILY_ING, 4:DAILY_DONE',
                `current_code` varchar(16) COMMENT 'when status is 1 or 3, this field is useful',
                `current_timerange` varchar(128) COMMENT 'where status is 1 or 3, this field is useful',
                PRIMARY KEY(`key`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
        ''',
        "downloadTaskStatus":'''
            create table `t_download_task_status` (
               `id` int(16) NOT NULL AUTO_INCREMENT,
               `first_run_time` datetime NOT NULL COMMENT 'first run time.',
               `last_run_time` datetime NOT NULL COMMENT 'last run time.',
               `run_times` int(8) NOT NULL COMMENT 'how many time does this task ran.',
               `options` varchar(1024) NOT NULL COMMENT 'the task cmd options.',
               `last_success_code` varchar(16) NOT NULL COMMENT 'last success stock code.',
               `last_success_start` date NOT NULL COMMENT 'start date.',
               `last_success_end` date NOT NULL COMMENT 'end date.',
               `update_time` datetime NOT NULL COMMENT 'last touch this record.',
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
            choices=["db_stock", "tbl_dailyHfqStock", "tbl_dailyQfqStock", "tbl_stockBasic","tbl_downloadTaskStatus", "tbl_sandboxStatus", "all"],
            help='''
Create the database or table. Case insensitive.
all : create all the needed database and tables.
db_stock : create the database.
tbl_dailyHfqStock : create the table of name "dailyHfqStock", which records the daily stock info, with hqf prices.
tbl_dailyQfqStock : create the table of name "dailyQfqStock", which records the daily stock info, with qfq prices.
tbl_stockBasic : create the table of name "stockBasic", which records the stock basic info.
tbl_downloadTaskStatus : create the table of name "downloadTaskStatus", which records the download task's status, to provide the breakpoint.
            ''', required=True)
    parser.add_argument("-f", "--force", action="store_true",
            help="if this option is set, will do init even if the db/table exists, which leads to data deleted.")

    args = parser.parse_args()

    if args.create:
        if args.create.lower() in ("db_stock", "all"):
            _createDB()
        if args.create.lower() in ("tbl_sandboxstatus", "all"):
            _createTbl("sandboxStatus")
        if args.create.lower() in ("tbl_dailyhfqstock", "all"):
            _createTbl("dailyHfqStock")
        if args.create.lower() in ("tbl_dailyqfqstock", "all"):
            _createTbl("dailyQfqStock")
        if args.create.lower() in ("tbl_fqFactor", "all"):
            _createTbl("fqFactor")
        if args.create.lower() in ("tbl_downloadtaskstatus", "all"):
            _createTbl("downloadTaskStatus")
        if args.create.lower() in ("tbl_stockBasic", "all"):
            _createTbl("stockBasic")

