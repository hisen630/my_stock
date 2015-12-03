#!/usr/bin/env python2.7
#coding=utf-8


import logging
import logging.config

import sys
sys.path.append('..')

import conf.conf as conf

logging.config.fileConfig('logging.conf')
if conf.DEBUG:
    logger = logging.getLogger('dev')
else:
    logger = logging.getLogger('product')
