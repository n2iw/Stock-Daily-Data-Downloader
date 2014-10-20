#!/usr/bin/env python

#
#  Copyright (c) 2011-2014, Ming James Ying N2IW n2iw@arrl.net
#  
#  get_historical_prices function is copied and modified from 
#  Corey Goldberg's ystockquote library (corey@goldb.org) thanks Corey! 
#
#  license: GNU LGPL
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#
# Download all stock's daily data and insert them into a MySQL database
#
# It will find out the last date in the database and only download new data
# But, when a stock has dividends or split, the Adj Close value of all previous
# data will change, so if it happens, all old data of this stock will be deleted
# from database, and all data will be downloaded again so we always have a correct
# Adj Close data in the database
#
# Database name and table name are defined as global constants
#
# Column names are the same as Yahoo's data header name
# Date, Open, High, Low, Close, Volume, Adj Close (Close adjusted for 
# dividends and splits)
#
# symbols are stored in a table in the database, 
# first Column must be the stock symbol
# column name is not important

import urllib
import MySQLdb  
import datetime
import os
from Queue import Queue
import threading
import getpass


# Global constants
database = "stocks"
dbuser = "stocks"
password = ""
dataTablename = "Days" # daily data table
symbolTableName = "Names" # symbol table

def get_historical_prices(symbol, start_date, end_date):
    """
    Get historical prices for the given ticker symbol.
    Date format is 'YYYYMMDD'
    
    Returns a nested list.
    """
    url = 'http://ichart.yahoo.com/table.csv?s=%s&' % symbol + \
          'd=%s&' % str(int(end_date[4:6]) - 1) + \
          'e=%s&' % str(int(end_date[6:8])) + \
          'f=%s&' % str(int(end_date[0:4])) + \
          'g=d&' + \
          'a=%s&' % str(int(start_date[4:6]) - 1) + \
          'b=%s&' % str(int(start_date[6:8])) + \
          'c=%s&' % str(int(start_date[0:4])) + \
          'ignore=.csv'
    days = urllib.urlopen(url).readlines()
    # Next line was modified by Ming James Ying from -2 to -1, 
    # to solve problem that cut the "Adj Close" column to "Adj Clos"
    # and also cut one digit from column "ADj Close"
    data = [day[:-1].split(',') for day in days]
    return data

#downloading threads
def processStocks(q, qo, startdate):
    cn = MySQLdb.connect(user = dbuser, db = database, passwd = password)
    cursor = cn.cursor()
    record_count = 0
    stock_count = 0
    updated_stocks = 0
    error_stocks = []
    today = "%04d%02d%02d" % (startdate.year, startdate.month, startdate.day)
 
    while not q.empty():
        symbol = q.get()
        nextdate = 0
        inserted_rows = 0
        #print 'Start to deal with: %s' % symbol

        #ignore stocks with a '.' in the symbol
        if '.' in symbol:
            #print '%s: skipped' % symbol
            continue

        # get last date for this symbol
        stock_count = stock_count + 1
        #print '%s: searching for last trading days in Database ...' % symbol
        cursor.execute('select max(date) from Days where Symbol="%s"' % symbol)
        lastday = cursor.fetchone()[0]
        if lastday:
            # download lastday again to findout if there is 
            # an adjust happened since then
            nextdate = lastday 
            nextday =  "%04d%02d%02d" % (nextdate.year, nextdate.month, 
                    nextdate.day)
        else:
            # download all the data
            nextdate = datetime.date(1000,01,01)
            nextday = "10000101"

        # skip if there are no new data to download
        if nextdate >= startdate:
            print "%s: no data to import  try again tomorrow!" % symbol
            continue

        print "%s: downloading from %s to %s " % (symbol, nextday, today)
        data = get_historical_prices(symbol, nextday, today)
        try:
            #get column names
            (name_date,name_o,name_h,name_l,name_c,name_vol,name_adj_close) \
                    = data[0]
            #print data[0]
            del data[0]

            #get data
            #print '%s: inserting data to Database ...' % symbol
            cmd_i = 'INSERT IGNORE INTO %s (Symbol,%s,%s,%s,%s,%s,%s,`%s`) \
                    VALUES' % (dataTablename, name_date, name_o, name_h, name_l,
                            name_c, name_vol, name_adj_close)
            for ddd in data:
                #print ddd
                (date, o, h, l, c, vol, adj_close) = ddd
                if nextday != "10000101" and c != adj_close :
                    cmd= 'DELETE FROM %s where Symbol="%s"' \
                            % (dataTablename, symbol)
                    cursor.execute(cmd)
                    cn.commit()
                    q.put(symbol)
                    updated_stocks = updated_stocks + 1
                    print '%s:%s Adj Close Changed! All data has to be updated'\
                            % (symbol, date)
                    raise Exception("All %s data have to be updated" % symbol)
                cmd_i += ' ("%s", "%s", %.2f, %.2f, %.2f, %.2f, %d, %.2f),'\
                        % (symbol,date, float(o), float(h), float(l),
                                float(c), int(vol), float(adj_close))
                #print cmd_i
                inserted_rows = inserted_rows + 1

            if inserted_rows > 1:
                # remove last ','
                cmd_i = cmd_i.rstrip(',')
                #print cmd_i
                cursor.execute(cmd_i)
                cn.commit()
                record_count = record_count + inserted_rows -1
            #print "%s: has %d new records" % (symbol, inserted_rows)
            

        except Exception, e:
            print "%s: ERROR:%s" % (symbol,e)
            error_stocks.append(symbol)
    qo.put( (stock_count, record_count, error_stocks, updated_stocks) )

def main():
    global password
    password = getpass.getpass('password:')
    starttime = datetime.datetime.now()

    cn = MySQLdb.connect(user = dbuser, db = database, passwd = password)
    cursor = cn.cursor()

    # fetch all symbols from database
    print 'Query Symbols ...'
    cursor.execute('select Symbol from %s' % symbolTableName)
    symbollist = cursor.fetchall()

    q = Queue(8000)

    #symbollist = [('LL',)]
    #symbollist = [('AAPL',),('LL',),('MLNX',)]

    #count = 0
    # put symbols into queue
    for ss in symbollist:
        (symbol,) = ss
        q.put(symbol)
        #count += 1
        #if count > 100:
        #    break

    tns = 8 # how many threads to use
    threads = []

    # out put queue
    qo = Queue(tns)

    record_count = 0
    stock_count = 0
    updated_stocks = 0
    error_stocks = []

    # start the threads
    for i in range(tns):
        t = threading.Thread(target = processStocks, 
                args = (q, qo, starttime.date())
                )
        threads.append(t)
        t.start()

    # wait for all threads to finish
    for i in range(tns):
        threads[i].join()

    # count data
    while not qo.empty():
        (stocks, records, errors, updated) = qo.get()
        record_count = record_count + records
        stock_count = stock_count + stocks
        updated_stocks = updated_stocks + updated
        for e in errors:
            error_stocks.append(e)

    print "-----------------------------"
    print "%s stocks processed" % stock_count
    print "%s stocks updated!" % (updated_stocks)
    print "%s errors occurred!" % len(error_stocks)
    for e in error_stocks:
        print e
    print "%s records inserted!" % (record_count)

    cursor.close()
    cn.close()
    dd = datetime.datetime.now()
    endtime = dd
    print "Started at: %s" % starttime
    print "Ended   at: %s" % endtime
    print "Used time %s" % (endtime - starttime)
    
if __name__ == '__main__':
    main()
