#!/usr/bin/env python2.7
#coding=utf-8

import time

DEV = True

############## mysql configuration ###############
PRODUCT_mysqlConfig = {
        "host":"192.168.222.128", 
        "port":3306, 
        "user":"root", 
        "passwd":"123456",
        "charset":"utf8"
        }

DEV_mysqlConfig = PRODUCT_mysqlConfig

mysqlConfig = PRODUCT_mysqlConfig if not DEV else DEV_mysqlConfig
DB_NAME = "stock" if not DEV else "dev_stock"
STOCK_BASIC_TABLE = "t_stock_basics"


STOCK_NUMBER_LOW_BOUND = 1000

import os
CWD = os.getcwd()
LOGGING_CONF_FILENAME = CWD + '/conf/logging.conf'


###################### initailizing logger ############ 
import logging
import logging.config

logging.config.fileConfig(LOGGING_CONF_FILENAME)
logger = logging.getLogger('DEV') if DEV else logging.getLogger('PRODUCT')
