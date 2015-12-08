#!/usr/bin/env python2.7
#coding=utf-8

import os
import sys
from tushare.util import dateu as du

import conf.conf as conf
from download_daily_data import DailyDataDownloader
from hfq2qfq import _hfq2qfq

if "__main__" == __name__:


    if du.is_holiday(du.today()):
        conf.logger.info("Today is holiday!") 
        sys.exit(0)

    
    dd = DailyDataDownloader()
    conf.logger.info("Start to download hfq data.")
    dd.download()
    conf.logging.info("hfq data downloaded.")

    conf.logger.info("Start to calculate qfq data.")
    _hfq2qfq()
    conf.logger.info("qfq data calculation completed.")

    conf.logger.info("Daily data downloaded successfully.")
