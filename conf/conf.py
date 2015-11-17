#!/usr/bin/env python2.7
#coding=utf-8

import os


# how to save downloaded data
saveMode = "mysql"

DB_NAME = "stock"
DAILY_STOCK_TABLE = "dailyStock"
STOCK_BASIC_TABLE = "stockBasic"

############## mysql configuration ###############
mysqlConfig = {
        "host":"192.168.222.128", "port":3306, "user":"root", "passwd":"123456"        }


############## system configuration ################
CWD = os.getcwd()
LOG_HOME = CWD + "/logs"
if not os.path.exists(LOG_HOME):
    os.mkdir(LOG_HOME)

