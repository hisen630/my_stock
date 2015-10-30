# my_stock

# Pre-requirements
1. Python 2.7 (Anaconda release is a better choice)
2. MySQL 5.5+
3. MySQL-python package (You could need to install the MySQL driver first)
4. tushare 0.9+ -- A python package

# Installation
1. Setup your MySQL properly.
2. Change $MY_STOCK_HOME/conf/conf.py to set up MySQL config
3. Init the MySQL database:  
   ```
   cd $MY_STOCK_HOME/script
   python initDB.py -c all
   ```

# Download stock data
Check this cmd:  
```
python dump_stock.py -h
```
