#Stock Daily Data Downloader

##Overview
Stock Daily Data Downloader is a python program that downloads stock daily price and volume data from Yahoo Finance website.

##Features

- Downloads daily price data of your stocks and insert data into MySQL database
- Automatically detect last downloaded data and only download new data
- Automatically detect dividend and/or split and make sure Adj Close fileld is up to date
- Multi thread downloading default 8 threads

##Data format

###Constants

Database name, database user name and table name are defined as global constants

- Database name: "database"
- Database user name: "dbuser"
- Daily data table: "dataTablename"
- Stock symbol list table: "symbolTableName"

###Column names

####Daily data table
Column names in daily data table should be the same as Yahoo's data header name

- Symbol
- Date
- Open
- High
- Low
- Close
- Volume
- Adj Close (Close adjusted for  dividends and splits)

####Symbol table


First Column in symbol table must be the stock symbol, you can have other columns after that if you want to.

Column name is not important.

stocks.sql is the database schema that I used, feel free to use it as is or modify it to your preference.

##About me

My personal website is n2iw.com 
