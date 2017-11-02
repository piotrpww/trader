#!/bin/python

import json
import requests
import sqlite3
import time
import math
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rcParams




def get_bitcoin_trades():
         inserted_rows = 0
         while True:
            db = sqlite3.connect("trader_bitfinex.sql3")
            c = db.cursor()
            c.execute("select max(timestamp) from btcusd_trades")
            local_max_timestamp = int(c.fetchone()[0]) - 1
            BITBAY_TRADES    =  "https://api.bitfinex.com/v1/trades/btcusd?timestamp=%s&limit_trades=1000" % int(local_max_timestamp)
            #time.sleep(1)           
            r = requests.get(BITBAY_TRADES)
            if r.status_code != 200 and r.status_code !=429:
                print ( "Server response code is %s" % r.status_code)
                break
            if r.status_code == 429: 
                "prekroczono ilosc requestow do serwera, czekanie..."
                time.sleep(2)
            tables = r.json()
            try:
                remote_max_timestamp=int(tables[-1]['timestamp'])
            except IndexError:
                break
            except KeyError as e:
                continue
            except UnboundLocalError as e:
                print(e)
            columns = ['timestamp', 'tid', 'price', 'amount', 'exchange', 'type']
            if len(tables) < 10:
                try:
                   return "[Stats] wprowadzono do bazy liczbe rekordow: %s, ostatni kurs: %s" % ( inserted_rows, tables[-1]['price'] )
                except KeyError as e:
                    continue
                break
            for element in tables:
                c = db.cursor()
                c.execute("select tid from btcusd_trades where tid=%s" % int(element['tid']) )
                temp_result = c.fetchone()
                if temp_result == None:
                    for dtype, value in element.iteritems():
                       keys = tuple(element[field] for field in columns)
                       query = "insert into btcusd_trades values (?, ?, ?, ?, ?, ?)"
                       c = db.cursor()
                       try:
                          c.execute(query, keys)
                          db.commit()
                          inserted_rows += 1
                          c.close()
                       except sqlite3.IntegrityError:
                          continue


def get_bitcoin_average_weight(period):
      current_time = int(time.time())
      fmin_before =  current_time - period*60*60
      db = sqlite3.connect("trader_bitfinex.sql3")
      c = db.cursor()

      c.execute("select timestamp, amount, price from btcusd_trades where timestamp > %s" % fmin_before)
      total_sum    = 0.0
      total_weight = 0.0
      for cos in c.fetchall():
              total_sum += float(cos[1])*float(cos[2])
              total_weight += float(cos[1])
      average_weight = total_sum/total_weight
      print average_weight
    

def get_bitcoin_avereage_windowed(av_period, flaga=1):


        db = sqlite3.connect("trader_bitfinex.sql3")
        c = db.cursor()

        c.execute("select max(timestamp) from btcusd_trades")
        max_data = int(c.fetchone()[0])
        max_time_in_table = max_data


        c.execute("select max(timestamp) from btcusd_trades_%s" % av_period)
        max_timestamp_calculated =  int(c.fetchone()[0])
        search_min_data = max_timestamp_calculated - (max_timestamp_calculated % av_period)

        if flaga == 0:   
            c.execute("select min(timestamp) from btcusd_trades")
            min_data = int(c.fetchone()[0])
            search_min_data =  min_data - ( min_data % av_period )
       
        search_max_data = search_min_data + av_period

        c = db.cursor()
        c.execute("select timestamp, amount, price from btcusd_trades where timestamp >= %s and timestamp < %s" % (search_min_data, search_max_data) )
 
        fetched_rows = c.fetchall()
        while search_max_data <= ( max_time_in_table ): 
           if fetched_rows != []:
                total_sum    = 0.0
                total_weight = 0.0

                for cos in fetched_rows:
                        total_sum += float(cos[1])*float(cos[2])
                        total_weight += float(cos[1])
                average_weight = total_sum/total_weight
                printed_data = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(search_min_data))
#                print printed_data, average_weight
                try:
                    db_ins = sqlite3.connect("trader_bitfinex.sql3")
                    c_ins = db_ins.cursor()
                    query_select = "SELECT timestamp from btcusd_trades_%s where timestamp=%s" % (av_period, search_min_data) 
                    c_ins.execute(query_select)
                    fetched_ins = c_ins.fetchone()
                    if fetched_ins == None:
                       query = "INSERT INTO btcusd_trades_%s(timestamp, rate ) values ( %s, %s ) " % ( av_period, search_min_data, average_weight )
                       #print query
                       c_ins.execute(query)
                       db_ins.commit()
                    db_ins.close()
                except sqlite3.IntegrityError as e:
                    print e
                    continue
                
           search_min_data = search_min_data + av_period
           search_max_data = search_min_data + av_period

           c = db.cursor()
           c.execute("select timestamp, amount, price from btcusd_trades where timestamp >= %s and timestamp < %s" % (search_min_data, search_max_data) )
           fetched_rows = c.fetchall()
                

def get_derivative(av_period, flag=1):

    if flag == 1:
     

      db = sqlite3.connect("trader_bitfinex.sql3")
      c = db.cursor()

      c.execute("SELECT timestamp, change_factor FROM btcusd_trades_%s order by timestamp asc LIMIT 1" % av_period)
      initial_record = c.fetchone()
      if initial_record[1] == None:
          c.execute("UPDATE btcusd_trades_%s SET change_factor=%s WHERE timestamp=%s" % (av_period, '0', initial_record[0]) )
          db.commit()
      
      c.execute("SELECT max(timestamp) FROM btcusd_trades_%s where change_factor IS NOT NULL" % av_period)
      max_timestamp = int(c.fetchone()[0])
      actual_timestamp = max_timestamp - av_period

      c.execute("SELECT * FROM btcusd_trades_%s WHERE timestamp >= %s  order by timestamp " % (av_period, actual_timestamp) ) 
      table = c.fetchall()

      curr_element = table[0]
      next_element = table[0]

      der_value = 0.0

      for element in table:
          next_element = element
          if next_element[0] != curr_element[0]:
             der_value = (next_element[1] - curr_element[1] ) / (next_element[0] - curr_element[0] )
             query_ins = "UPDATE btcusd_trades_%s SET change_factor = %s WHERE timestamp = %s" % (av_period, der_value, element[0] )
             c.execute(query_ins)
             db.commit()
    #      else:
    #         der_value = 0
    #         query_ins = "UPDATE btcusd_trades_%s SET change_factor = %s WHERE timestamp = %s" % (av_period, der_value, element[0] )
    #         c.execute(query_ins)
    #         db.commit()
    #      print element[0], element[1], der_value
          curr_element = next_element
      

def calculate_score(time_basis='now'):
    
    if time_basis == 'now':

        #print "-------------------------------------------------------------"
#     for timestamp    
#1        
        db = sqlite3.connect("trader_bitfinex.sql3")
        c = db.cursor()
        c.execute("SELECT * FROM btcusd_trades_30 ORDER BY timestamp desc LIMIT 10")
        results = c.fetchall()
######################################################################################
        #V1
        positive_30_v1_counter = 0
        first_30_v1_value = 0.0
        positive_30_v1_change = 0.0
        derivative_30_v1_sum = 0.0 

        for counter in range(9,-1,-1):
            #print results[counter]
            derivative_30_v1_sum += results[counter][2]
            
            if results[counter][2] > 0.0:
                positive_30_v1_counter +=1
            else:
                positive_30_v1_counter -=1

        first_30_v1_value = results[0][1] - results[9][1]
        

        #v2 ##########
        positive_30_v2_counter = 0
        first_30_v2_value = 0.0
        positive_30_v2_change = 0.0
        derivative_30_v2_sum = 0.0 


        for counter in range(5,-1,-1):
          #print results[counter]
          derivative_30_v2_sum += results[counter][2]

          if results[counter][2] > 0.0:
              positive_30_v2_counter +=1
          else:
              positive_30_v2_counter -=1
        first_30_v2_value = results[0][1] - results[5][1]
        #v3 ###########
        positive_30_v3_counter = 0
        first_30_v3_value = 0.0
        positive_30_v3_change = 0.0
        derivative_30_v3_sum = 0.0 

        for counter in range(9,4,-1):
          #print results[counter]
          derivative_30_v3_sum += results[counter][2]

          if results[counter][2] > 0.0:
              positive_30_v3_counter +=1
          else:
              positive_30_v3_counter -=1
        first_30_v3_value = results[5][1] - results[9][1]


 #       print "positive30=%s, derivative30=%s, diff =%s" % (positive_30_v1_counter, derivative_30_v1_sum, first_30_v1_value)
 #       print "positive30_v2=%s, derivative30_v2=%s, diff=%s" % (positive_30_v2_counter, derivative_30_v2_sum, first_30_v2_value)
 #       print "positive30_v3=%s, derivative30_v3=%s diff=%s" % (positive_30_v3_counter, derivative_30_v3_sum, first_30_v3_value)
 #       print "-----------------------------------------------------------"
#2
        c.execute("SELECT * FROM btcusd_trades_300 ORDER BY timestamp desc LIMIT 10")
        results = c.fetchall()
        positive_300_counter = 0
        first_300_value = 0.0
        positive_300_change = 0.0
        derivative_300_sum = 0.0

        for counter in range(4,-1,-1):
            #print results[counter]
            derivative_300_sum += results[counter][2]

            if results[counter][2] > 0.0:
                positive_300_counter +=1
            else:
                positive_300_counter -=1
        first_300_value = results[0][1] - results[4][1]
  #      print "positive300=%s, derivative300=%s, diff: %s" % (positive_300_counter, derivative_300_sum, first_300_value)
  #      print "-------------------------------------------------------------"

#3
        c.execute("SELECT * FROM btcusd_trades_1200 ORDER BY timestamp desc LIMIT 10")
        results = c.fetchall()
        positive_1200_counter = 0
        first_1200_value = 0.0
        positive_1200_change = 0.0
        derivative_1200_sum = 0.0

        for counter in range(2,-1,-1):
            #print counter
            #print results[counter]
            derivative_1200_sum += results[counter][2]

            if results[counter][2] > 0.0:
                positive_1200_counter +=1
            else:
                positive_1200_counter -=1
        first_1200_value = results[0][1] - results[1][1]
   #     print "positive1200=%s, derivative1200=%s, diff: %s" % (positive_1200_counter, derivative_1200_sum, first_1200_value)
   #     print "-------------------------------------------------------------"
       

        # 
        weight_short_period = 2.0
        weight_medium_period = 1.0
        weight_long_period = 0.8


        score = weight_short_period * (    ( 10 * positive_30_v1_counter / 10.0 + 5 * positive_30_v2_counter + 20 * positive_30_v3_counter )  + 2*first_30_v1_value + first_30_v2_value+ 5*first_30_v3_value ) + weight_medium_period * (positive_300_counter  + first_300_value)
    #    print "Score: %s" %score
    return score



while True:

    while True:
          trades = get_bitcoin_trades()
          #get_bitcoin_average_weight(0.5)
          #get_bitcoin_average_weight(3)
          #get_bitcoin_average_weight(6)

          get_bitcoin_avereage_windowed(30)
          get_bitcoin_avereage_windowed(300)
          get_bitcoin_avereage_windowed(600)
          get_bitcoin_avereage_windowed(1200)
          get_bitcoin_avereage_windowed(1800)
          get_bitcoin_avereage_windowed(3600)
          get_bitcoin_avereage_windowed(21600)
          
          get_derivative(30)
          get_derivative(300)
          get_derivative(600)
          get_derivative(1200)
          get_derivative(1800)
          get_derivative(3600)
#
          score = calculate_score()

          if abs(score) > 1000: 
            print "last price: %s, Score: %s" % (score, trades)


          time.sleep(10)




          break
#          trades_sum=0
#          con = sqlite3.connect('btc_trading.slite')
#          cur = con.cursor()
#          cur.execute('SELECT SQLITE_VERSION()')
