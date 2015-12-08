# my_stock

# Pre-requirements
1. Python 2.7 (Anaconda release is a better choice)
2. MySQL 5.5+
3. MySQL-python package (You could need to install the MySQL driver first)
4. tushare 0.9+ -- A python package for downloading stock data
5. dateutil -- A python package for handling date strings

# Installation
1. Setup your MySQL properly.
2. Change $MY_STOCK_HOME/conf/conf.py to set up MySQL config
3. Init the MySQL database:  
   ```
   cd $MY_STOCK_HOME/script
   python initDB.py -c all
   ```

# Download history stock data
# for the first time to download data, and if missed some data, use this to get them back.
Check this cmd:  
```
python download_history_data.py -h
```

