# -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 19:36:33 2019
@author: Galileo
外資均價
"""
import pandas as pd

def craw_fiin(stock_number, c_date):
    try:
        dfInout = pd.read_csv('inout\\' + c_date + '.csv', thousands = ',')
        dfInout = dfInout[dfInout['證券代號'] == str(stock_number)]
        dfInout = dfInout.reset_index(drop = True)

        if float(c_date) < 20171218:
            fiin = dfInout.at[0, '外資買進股數']
            
        else:
            fiin = dfInout.at[0, '外陸資買進股數(不含外資自營商)']

        return fiin
    
    except Exception:
        return -1

gListGrp = ['stock_50', 'stock_100', 'stock_etf', 'stock_add']

for g in gListGrp:
    gDfStock = pd.read_csv(g + '.csv')    
    gTickers = list(gDfStock.loc[:, '股票代號'])  
    
    for i in gTickers:
        print(i)
        gDfClean = pd.read_csv('clean\\' + str(i) + '.TW_CLEAN.csv')
        gDfClean = gDfClean[gDfClean['日期'] >= "101/05/02"]
        gDfFi = gDfClean.reset_index(drop = True)
        gDfFi['fiin'] = 0
        gDfFi['Temp'] = 0

        rows = gDfFi.shape[0]
        
        for j in range(rows - 1):
            d = gDfFi.at[j, '日期']
            d = str(int(d[0:3])+ 1911) + d[4:6] + d[7:9]

            fiin = craw_fiin(i, d)
            gDfFi['fiin'][j] = fiin
            
            close = gDfFi['收盤價'][j]
            gDfFi['Temp'][j] = float(fiin) * float(close)
            
        gDfFi.to_csv(r'D:\python\stock\fi\\' + str(i) + '.TW_FI_ORI.csv')
        gDfFi = gDfFi[gDfFi['fiin'] > -1]
        gDfFi.to_csv(r'D:\python\stock\fi\\' + str(i) + '.TW_FI.csv')
        gDfFi.describe().to_csv(r'D:\python\stock\fi\\' + str(i) + '.TW_FI_DESC.csv')