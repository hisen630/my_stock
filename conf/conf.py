#!/usr/bin/env python2.7
#coding=utf-8

import os


# how to save downloaded data
saveMode = "mysql"

############## mysql configuration ###############
mysqlConfig = {
        "host":"127.0.0.1", "port":3306, "user":"hisen", "passwd":"123456", "db":"stock"
        }

DB_NAME = "stock"
DAILY_STOCK_TABLE = "dailyStock"
STOCK_BASIC_TABLE = "stockBasic"

############## system configuration ################
CWD = os.getcwd()
LOG_HOME = CWD + "/logs"
if not os.path.exists(LOG_HOME):
    os.mkdir(LOG_HOME)

