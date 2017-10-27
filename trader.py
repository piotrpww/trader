#!/bin/python

import json
import requests
import sqlite3
import time




def get_bitcoin_trades():
        db = sqlite3.connect("trader.sql3")
        c = db.cursor()
        c.execute("select max(tid) from btcpln_trades")
        max_tid = int(c.fetchone()[0]) 
        BITBAY_TRADES    =  "https://bitbay.net/API/Public/BTCPLN/trades.json?since=%s" % int(max_tid)
        r = requests.get(BITBAY_TRADES)
        tables = r.json()
        columns = ['date', 'price', 'type', 'amount', 'tid']
        for element in tables:
            print element
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
    

#def get_bitcoin_average(period)
     



while True:
          BITBAY_TRADES    =  "https://bitbay.net/API/Public/BTCPLN/trades.json?sort=desc"
          BITBAY_ORDERBOOK =  "https://bitbay.net/API/Public/BTCPLN/orderbook.json?sort=desc"
          trades = get_bitcoin_trades()
          get_bitcoin_average_weight(0.5)
          get_bitcoin_average_weight(3)
          get_bitcoin_average_weight(6)
          break
#          trades_sum=0
#          con = sqlite3.connect('btc_trading.slite')
#          cur = con.cursor()
#          cur.execute('SELECT SQLITE_VERSION()')
