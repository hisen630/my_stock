
import os
import sys
from tushare.util import dateu as du

if "__main__" == __name__:


    if du.is_holiday(du.today()):
        pass
    else:
        ret = os.system('/home/work/anaconda/bin/python ./download_daily_data.py -p >>logs/daily.log 2>>logs/daily.err')
        if ret != 0:
            print >> sys.stderr, 'daily error with exit code %d'%ret
        else:
            ret = os.system('/home/work/anaconda/bin/python ./hfq2qfq.py -p >>logs/fq.log 2>>logs/fq.err')
            if ret != 0:
                print >> sys.stderr, 'fq error with exit code %d'%ret

