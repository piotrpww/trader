#!/bin/python

import json
import requests
import sqlite3
import time
import math




def get_bitcoin_trades():
         while True:
            db = sqlite3.connect("trader.sql3")
            c = db.cursor()
            c.execute("select max(tid) from btcpln_trades")
            local_max_tid = int(c.fetchone()[0]) 
            print local_max_tid
            BITBAY_TRADES    =  "https://bitbay.net/API/Public/BTCPLN/trades.json?since=%s" % int(local_max_tid)
            r = requests.get(BITBAY_TRADES)
            tables = r.json()
            try:
                remote_max_tid=int(tables[-1]['tid'])
            except IndexError:
                break
            if ( local_max_tid - remote_max_tid ) == 0:
                break
            columns = ['date', 'price', 'type', 'amount', 'tid']
            for element in tables:
                for dtype, value in element.iteritems():
                   keys = tuple(element[field] for field in columns)
                query = "insert into btcpln_trades values (?, ?, ?, ?, ?)"
                c = db.cursor()
                c.execute(query, keys)
            c.close()
            db.commit()



def get_bitcoin_average_weight(period):
      current_time = int(time.time())
      fmin_before =  current_time - period*60*60
      db = sqlite3.connect("trader.sql3")
      c = db.cursor()

      c.execute("select date, amount, price from btcpln_trades where date > %s" % fmin_before)
      total_sum    = 0.0
      total_weight = 0.0
      for cos in c.fetchall():
              total_sum += float(cos[1])*float(cos[2])
              total_weight += float(cos[1])
      average_weight = total_sum/total_weight
      print average_weight
    

def get_bitcoin_avereage_windowed(av_period, flaga=1):


    if flaga == 0:
        db = sqlite3.connect("trader.sql3")
        c = db.cursor()
        c.execute("select min(date) from btcpln_trades")
        min_data = int(c.fetchone()[0])
        
        search_min_data =  min_data - ( min_data % av_period )
        search_max_data = search_min_data + av_period
  
        
        c = db.cursor()
        c.execute("select date, amount, price from btcpln_trades where date >= %s and date < %s" % (search_min_data, search_max_data) )
        
        total_sum    = 0.0
        total_weight = 0.0
         
        for cos in c.fetchall():
                total_sum += float(cos[1])*float(cos[2])
                total_weight += float(cos[1])
        average_weight = total_sum/total_weight
        print average_weight


    if flaga == 1:

        db = sqlite3.connect("trader.sql3")
        c = db.cursor()
        c.execute("select min(date) from btcpln_trades")
        min_data = int(c.fetchone()[0])
        c.execute("select max(date) from btcpln_trades")
        max_data = int(c.fetchone()[0])


        search_min_data =  min_data - ( min_data % av_period )
        search_max_data = search_min_data + av_period

        
        c = db.cursor()
        c.execute("select date, amount, price from btcpln_trades where date >= %s and date < %s" % (search_min_data, search_max_data) )
 
        fetched_rows = c.fetchall()
        while fetched_rows != []: 
                total_sum    = 0.0
                total_weight = 0.0

                for cos in fetched_rows:
                        total_sum += float(cos[1])*float(cos[2])
                        total_weight += float(cos[1])
                average_weight = total_sum/total_weight
                printed_data = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(search_min_data))
                print printed_data, average_weight
                
                search_min_data = search_min_data + av_period
                search_max_data = search_min_data + av_period

                c = db.cursor()
                c.execute("select date, amount, price from btcpln_trades where date >= %s and date < %s" % (search_min_data, search_max_data) )
                fetched_rows = c.fetchall()






while True:
          BITBAY_TRADES    =  "https://bitbay.net/API/Public/BTCPLN/trades.json?sort=desc"
          BITBAY_ORDERBOOK =  "https://bitbay.net/API/Public/BTCPLN/orderbook.json?sort=desc"
          #trades = get_bitcoin_trades()
          #get_bitcoin_average_weight(0.5)
          #get_bitcoin_average_weight(3)
          #get_bitcoin_average_weight(6)

          get_bitcoin_avereage_windowed(1200)


          break
#          trades_sum=0
#          con = sqlite3.connect('btc_trading.slite')
#          cur = con.cursor()
#          cur.execute('SELECT SQLITE_VERSION()')
