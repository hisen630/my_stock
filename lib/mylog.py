#!/usr/bin/env python2.7
#coding=utf-8


import logging
import sys
sys.path.append("..")
import conf.conf as conf

logging.basicConfig(filename='%s/run.log'%conf.LOG_HOME, 
        format="[%(levelname)s]\t[%(asctime)s]\t%(message)s",
        level=logging.DEBUG)
logging.info("Logging configured.")
