#!/usr/bin/env python2.7
#coding=utf-8


import logging
import logging.config

import sys
sys.path.append("..")
import conf.conf as conf

logging.config.fileConfig('../conf/logging.conf')
#logging.config.fileConfig('../conf/fuck.conf')
if conf.DEV:
    logger = logging.getLogger('DEV')
else:
    logger = logging.getLogger('PRODUCT')

