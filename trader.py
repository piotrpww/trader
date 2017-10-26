#!/bin/python

import json
import requests
import sqlite3
import time


def get_bitcoin_trades(http_api, last_transaction):
        r = requests.get(http_api)
        curr = r.json()[1]
        curr_date = r.json()[1]['date']
        date_sum = 0
        next_date = 0
        for event in reversed(r.json()):
            print event
            next_event = event
            date_sum += ( next_event['date'] - curr['date'] )
            curr = event
        print date_sum

while True:
          BITBAY_TRADES    =  "https://bitbay.net/API/Public/BTCPLN/trades.json?sort=desc"
          BITBAY_ORDERBOOK =  "https://bitbay.net/API/Public/BTCPLN/orderbook.json?sort=desc"
          trades = get_bitcoin_trades(BITBAY_TRADES, '1895840')
          time.sleep(3)
          break
#          trades_sum=0
#          con = sqlite3.connect('btc_trading.slite')
#          cur = con.cursor()
#          cur.execute('SELECT SQLITE_VERSION()')

