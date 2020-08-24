    # -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 19:36:33 2019
@author: Galileo
Download、Clean、Desc
"""
from datetime import date
from urllib.request import urlopen
from dateutil import rrule
import datetime
import pandas as pd
import json
import time

class stock:
    def __init__(self, stock_number, start_month):
        self.stock_number = stock_number
        self.start_month = start_month
        
    # 根據使用者輸入的日期，以月為單位，重複呼叫爬取月股價的函式
    def dl_craw_stock(self):
        b_month = date(*[int(x) for x in self.start_month.split('-')])
        now = datetime.datetime.now().strftime("%Y-%m-%d")         # 取得現在時間
        e_month = date(*[int(x) for x in now.split('-')])
        
        for d in rrule.rrule(rrule.MONTHLY, dtstart = b_month, until = e_month):
            print(d)
            self.dl_craw_one_month(d)
    
            time.sleep(2500.0/1000.0);
            
    # 爬取每月股價的目標網站並包裝成函式     
    def dl_craw_one_month(self, date):
        for i in range(2):
            try:
                strDate = date.strftime('%Y%m%d')
                url = (
                    "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=" +
                    strDate + "&stockNo=" + str(self.stock_number)
                )

                data = json.loads(urlopen(url).read())
    
                if data['stat'] == 'OK':
                    dfStock = pd.DataFrame(data['data'], columns = data['fields'])
                    dfStock.set_index("日期", inplace = True)
                    dfStock.to_csv(r'D:\python\stock\his\\' + str(self.stock_number) + '\\' + str(self.stock_number) + '.TW.' + strDate + '.csv')
                    
                    return
    
                time.sleep(2500.0/1000.0);
                print(str(i) + ', sleep(2.5)')
                
            except Exception:
                pass
            
    # 根據使用者輸入的日期，以月為單位，重複呼叫爬取月股價的函式
    def clean_craw_stock(self):
        b_month = date(*[int(x) for x in self.start_month.split('-')])
        now = datetime.datetime.now().strftime("%Y-%m-%d")         # 取得現在時間
        e_month = date(*[int(x) for x in now.split('-')])
        
        dfStock = pd.DataFrame()
        
        for d in rrule.rrule(rrule.MONTHLY, dtstart = b_month, until = e_month):
            dfStock = pd.concat([dfStock, s.clean_craw_one_month(d)], ignore_index = True)
            
        return dfStock
    
    # 爬取每月股價的目標網站並包裝成函式
    def clean_craw_one_month(self, date):
        try:
            strDate = date.strftime('%Y%m%d')
            dfTmp = pd.read_csv('his\\' + str(self.stock_number) + '\\' + str(self.stock_number) + '.TW.' + strDate + '.csv', thousands = ',')
            
            return dfTmp
        
        except Exception:
            pass
    
def dl_inout():
    for i in range(7):
        try:
            gdate = datetime.date.today() - datetime.timedelta(days = i)
            print(str(gdate))
            
            timeStruct = time.strptime(str(gdate), "%Y-%m-%d")
            strTime = time.strftime("%Y%m%d", timeStruct)
            craw_inout(strTime)
            
        except Exception:
            pass

def craw_inout(date):
    url = (
        'http://www.tse.com.tw/fund/T86?response=json&date=' + date + '&selectType=ALLBUT0999'
    )
    
    data = json.loads(urlopen(url).read())
    
    dfInout = pd.DataFrame(data['data'], columns=data['fields'])
    dfInout.to_csv(r'D:\python\stock\inout\\' + date + '.csv')
    
gListGrp = ['stock_50', 'stock_100', 'stock_etf', 'stock_add']

for g in gListGrp:
    gDfStock = pd.read_csv(g + '.csv')     
    gTickers = list(gDfStock.loc[:, '股票代號']) 

    for i in gTickers:
        print(i)
        #DL
        s = stock(i, "2020-08-01")
        s.dl_craw_stock()    
        
        #Clean
        s.start_month = "2012-08-01"
        gDfHis = s.clean_craw_stock()
        
        gDfClean = gDfHis.loc[:, ['日期', '開盤價', '最高價', '最低價', '收盤價']]
        
        gDfHis.set_index("日期", inplace = True)
        gDfHis.to_csv(r'D:\python\stock\his\\' + str(i) + '.TW.csv')
        
        gDfClean = gDfClean[gDfClean['開盤價'] != '--']
        gDfClean.set_index("日期", inplace = True)
        gDfClean.to_csv(r'D:\python\stock\clean\\' + str(i) + '.TW_CLEAN.csv')
    
        #DESC
        gDfClean = pd.read_csv('clean\\' + str(i) + '.TW_CLEAN.csv', thousands = ',')
        print(gDfClean.describe())
        gDfClean.describe().to_csv(r'D:\python\stock\desc\\' + str(i) + '.TW_DESC.csv')
        gDfClean.to_csv(r'D:\python\stock\clean\\' + str(i) + '.TW_CLEAN.csv')
    
dl_inout()