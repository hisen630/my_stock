#!/usr/bin/env python2.7
#coding=utf-8

import os

import time

DEV = True

ISO_TIME_FORMAT='%Y-%m-%d %X'
ISO_DATE_FORMAT='%Y-%m-%d'

# for distingish running instances
sandbox_key="mls_pc_virtual-hhy-001"

# how to save downloaded data
saveMode = "mysql"

DB_NAME = "stock"
STOCK_BASIC_TABLE = "t_stock_basics"

STOCK_NUMBER_LOW_BOUND = 1000

############## mysql configuration ###############
mysqlConfig = {
        "host":"192.168.222.128", 
        "port":3306, 
        "user":"root", 
        "passwd":"123456",
        "charset":"utf8"
        }


############## system configuration ################
CWD = os.getcwd()
LOG_HOME = CWD + "/logs"
if not os.path.exists(LOG_HOME):
    os.mkdir(LOG_HOME)

