# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 19:36:33 2019
@author: Galileo
下載指定區間的股票歷史資料
"""
from datetime import date
from urllib.request import urlopen
from dateutil import rrule
import datetime
import pandas as pd
import json
import time

# 根據使用者輸入的日期，以月為單位，重複呼叫爬取月股價的函式
def craw_stock(stock_number, start_month):
    b_month = date(*[int(x) for x in start_month.split('-')])
    now = datetime.datetime.now().strftime("%Y-%m-%d")         # 取得現在時間
    e_month = date(*[int(x) for x in now.split('-')])
    
    for d in rrule.rrule(rrule.MONTHLY, dtstart=b_month, until = e_month):
        print(d)
        craw_one_month(stock_number, d)

        time.sleep(2500.0/1000.0);
        
# 爬取每月股價的目標網站並包裝成函式
def craw_one_month(stock_number, date):
    for i in range(2):
        try:
            strDate = date.strftime('%Y%m%d')
            url = (
                "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=" +
                strDate +
                "&stockNo=" +
                str(stock_number)
            )

            data = json.loads(urlopen(url).read())

            if data['stat'] == 'OK':
                dfStock = pd.DataFrame(data['data'], columns=data['fields'])
                dfStock.set_index("日期", inplace = True)
                dfStock.to_csv(r'D:\python\stock\his\\' + str(stock_number) + '\\' + str(stock_number) + '.TW.' + strDate + '.csv')
                
                return

            time.sleep(2500.0/1000.0);
            print(str(i) + ', sleep(2.5)')
            
        except Exception:
            pass

gDfStock = pd.read_csv('stock_etf.csv')    
gTickers = list(gDfStock.loc[:, '股票代號'])  

for i in gTickers:
    print(i)
    craw_stock(i, "2012-02-01")

    