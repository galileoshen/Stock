    # -*- coding: utf-8 -*-
"""
Created on Fri Nov 15 19:36:33 2019
@author: Galileo
策略
"""
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

class str1:
    def __init__(self, gTickers, grp):
        self.gTickers = gTickers
        self.grp = grp
            
    #策略: <=25%    
    def strUnder25(self):
        print('============================================================')
        print('策略: <=25%')
        print('============================================================')
    
        f = open('stock_25%_' + self.grp + '.txt','w')
        f.write('============================================================\n')
        f.write('策略: <=25%\n')
        f.write('============================================================\n')
        
        #cnt = 0
        
        for i in self.gTickers:
            #try:
                dfDesc = pd.read_csv('desc\\' + str(i) + '.TW_DESC.csv') 
                dfClean = pd.read_csv('clean\\' + str(i) + '.TW_CLEAN.csv')
                
                close25 = dfDesc.at[4, '收盤價']   #取25%關盤 

                row = dfClean.shape[0]
                close = dfClean.at[row - 1, '收盤價']

                if float(close25) >= float(close):
                    #cnt = cnt + 1

                    gDfStats['lt25'][i] = 'Y'
                    
                    dfTmp = gDfStock[gDfStock['股票代號'] == i]
                    
                    print(dfTmp)
                    f.write(dfTmp.to_string(index = False) + '\n')
                    
                    print('25%: ' + str(close25))
                    f.write('25%: ' + str(close25) + '\n')
                    
                    dfFi = pd.read_csv('fi\\' + str(i) + '.TW_FI_DESC.csv')
                    fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
                    
                    print('外資均價: ' + str(fi))
                    f.write('外資均價: ' + str(fi) + '\n')
                    
                    print('今日收盤: ' + str(close))
                    f.write('今日收盤: ' + str(close) + '\n')
                    
                    #畫圖
                    dfClean['收盤價'].plot()
                    close50 = dfDesc.at[5, '收盤價']   #取50%關盤
                    close75 = dfDesc.at[6, '收盤價']   #取50%關盤
                    closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
                    
                    plt.axhline(close25, color= 'r')
                    plt.axhline(close50, color= 'r')
                    plt.axhline(close75, color= 'r')
                    plt.axhline(closeMean, color= 'y')
                    plt.show()
                    
                    print(dfDesc)
                    f.write(dfDesc.to_string(index = False) + '\n')
                    
                    print('股利政策')
                    f.write('股利政策\n')
                    
                    dfPublish = gDfPublish[gDfPublish['代號'].str.strip() == str(i)]
                    dfPublish = dfPublish.reset_index(drop = True)
                    
                    print(dfPublish)
                    f.write(dfPublish.to_string(index = False) + '\n')
                    
                    print('----------------------------------------------------------------------------------------------------------------')
                    f.write('----------------------------------------------------------------------------------------------------------------\n')
                    
                    #if cnt >= 20:
                    #    os.system("pause");
                    #    cnt = 0
                    
            #except Exception:
                #pass
             
    #策略: <=50%    
    def strUnder50(self):
        print('============================================================')
        print('策略: <=50%')
        print('============================================================')
    
        f = open('stock_50%_' + self.grp + '.txt','w')
        f.write('============================================================\n')
        f.write('策略: <=50%\n')
        f.write('============================================================\n')
        
        #cnt = 0
        
        for i in self.gTickers:
            #try:
                dfDesc = pd.read_csv('desc\\' + str(i) + '.TW_DESC.csv') 
                dfClean = pd.read_csv('clean\\' + str(i) + '.TW_CLEAN.csv')
                
                close50 = dfDesc.at[5, '收盤價']   #取50%關盤 

                row = dfClean.shape[0]
                close = dfClean.at[row - 1, '收盤價']

                if float(close50) >= float(close):
                    #cnt = cnt + 1

                    gDfStats['lt50'][i] = 'Y'
                    
                    dfTmp = gDfStock[gDfStock['股票代號'] == i]
                    
                    print(dfTmp)
                    f.write(dfTmp.to_string(index = False) + '\n')
                    
                    print('50%: ' + str(close50))
                    f.write('50%: ' + str(close50) + '\n')
                    
                    dfFi = pd.read_csv('fi\\' + str(i) + '.TW_FI_DESC.csv')
                    fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
                    
                    print('外資均價: ' + str(fi))
                    f.write('外資均價: ' + str(fi) + '\n')
                    
                    print('今日收盤: ' + str(close))
                    f.write('今日收盤: ' + str(close) + '\n')
                    
                    #畫圖
                    dfClean['收盤價'].plot()
                    close25 = dfDesc.at[4, '收盤價']   #取50%關盤
                    close75 = dfDesc.at[6, '收盤價']   #取50%關盤
                    closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
                    
                    plt.axhline(close25, color= 'r')
                    plt.axhline(close50, color= 'r')
                    plt.axhline(close75, color= 'r')
                    plt.axhline(closeMean, color= 'y')
                    plt.show()
                    
                    print(dfDesc)
                    f.write(dfDesc.to_string(index = False) + '\n')
                    
                    print('股利政策')
                    f.write('股利政策\n')
                    
                    dfPublish = gDfPublish[gDfPublish['代號'].str.strip() == str(i)]
                    dfPublish = dfPublish.reset_index(drop = True)
                    
                    print(dfPublish)
                    f.write(dfPublish.to_string(index = False) + '\n')
                    
                    print('----------------------------------------------------------------------------------------------------------------')
                    f.write('----------------------------------------------------------------------------------------------------------------\n')
                    
                    #if cnt >= 20:
                    #    os.system("pause");
                    #    cnt = 0
                    
            #except Exception:
                #pass                

    #策略: 突破5日高點    
    def str5dHigher(self):
        print('=======================================================')
        print('策略: 突破5日高點1%')
        print('=======================================================')
    
        f = open('stock_5d_' + self.grp + '.txt','w')
        f.write('=======================================================\n')
        f.write('策略: 突破5日高點1%\n')
        f.write('=======================================================\n')
        
        for i in self.gTickers:
            #try:
                dfClean = pd.read_csv('clean\\' + str(i) + '.TW_CLEAN.csv')
                row = dfClean.shape[0]
                high = dfClean.at[row - 1, '最高價']
                cnt= 0
                
                for j in range(4):
                    tmp = dfClean.at[row - j - 2, '最高價']
                    if tmp * 1.01 >= high:
                        break
                        
                    cnt = cnt + 1
                    
                if cnt >= 4:
                    gDfStats['st5d'][i] = 'Y'
                    dfTmp = gDfStock[gDfStock['股票代號'] == i]
                    
                    print(dfTmp)
                    f.write(dfTmp.to_string(index = False) + '\n')
                    
                    close = dfClean.at[row - 1, '收盤價']
                    
                    print('今日高點: ' + str(high))
                    f.write('今日高點: ' + str(high) + '\n')
                    
                    print('今日收盤: ' + str(close))
                    f.write('今日收盤: ' + str(close) + '\n')
                    
                    print('%: ' + str(high / tmp))
                    f.write('%: ' + str(high / tmp) + '\n')
                    
                    dfDesc = pd.read_csv('desc\\' + str(i) + '.TW_DESC.csv') 
                    close25 = dfDesc.at[4, '收盤價']   #取25%關盤 
                    close50 = dfDesc.at[5, '收盤價']   #取50%關盤
                    close75 = dfDesc.at[6, '收盤價']   #取50%關盤
                    closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
                    
                    print('25%: ' + str(close25))
                    f.write('25%: ' + str(close25) + '\n')
                    print('50%: ' + str(close50))
                    f.write('50%: ' + str(close50) + '\n')
                    
                    dfFi = pd.read_csv('fi\\' + str(i) + '.TW_FI_DESC.csv')
                    fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
                    
                    print('外資均價: ' + str(fi))
                    f.write('外資均價: ' + str(fi) + '\n')
                    
                    #畫圖
                    dfClean['收盤價'].plot()
                    plt.axhline(close25, color= 'r')
                    plt.axhline(close50, color= 'r')
                    plt.axhline(close75, color= 'r')
                    plt.axhline(closeMean, color= 'y')
                    plt.show()
                    
                    print('-------------------------------------------------------')
                    f.write('-------------------------------------------------------\n')
                    
            #except Exception:
                #pass
            
        f.close() 
        
    #策略: 跳空    
    def strJump(self):
        print('=======================================================')
        print('策略: 跳空1%')
        print('=======================================================')
    
        f = open('stock_jump_' + self.grp + '.txt','w')
        f.write('=======================================================\n')
        f.write('策略: 跳空1%\n')
        f.write('=======================================================\n')
        
        for i in self.gTickers:
            #try:
                dfClean = pd.read_csv('clean\\' + str(i) + '.TW_CLEAN.csv')
                
                row = dfClean.shape[0]
                openT = dfClean.at[row - 1, '開盤價']
                closeY = dfClean.at[row - 2, '收盤價']
                pc = openT / closeY
                
                if openT > closeY and pc > 1.01:
                    gDfStats['stjp'][i] = 'Y'
                    dfTmp = gDfStock[gDfStock['股票代號'] == i]
                    
                    print(dfTmp)
                    f.write(dfTmp.to_string(index = False) + '\n')
                    
                    closeT = dfClean.at[row - 1, '收盤價']
                    
                    print('今日開盤: ' + str(openT))
                    f.write('今日開盤: ' + str(openT) + '\n')
                    print('昨日收盤: ' + str(closeY))
                    f.write('昨日收盤: ' + str(closeY) + '\n')
                    print('今日收盤: ' + str(closeT))
                    f.write('今日收盤: ' + str(closeT) + '\n')
                    print('%: ' + str(pc))
                    f.write('%: ' + str(pc) + '\n')
                    
                    dfDesc = pd.read_csv('desc\\' + str(i) + '.TW_DESC.csv') 
                    close25 = dfDesc.at[4, '收盤價']   #取25%關盤 
                    close50 = dfDesc.at[5, '收盤價']   #取50%關盤
                    close75 = dfDesc.at[6, '收盤價']   #取50%關盤
                    closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
                    
                    print('25%: ' + str(close25))
                    f.write('25%: ' + str(close25) + '\n')
                    print('50%: ' + str(close50))
                    f.write('50%: ' + str(close50) + '\n')
                    
                    dfFi = pd.read_csv('fi\\' + str(i) + '.TW_FI_DESC.csv')
                    fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
                    print('外資均價: ' + str(fi))
                    f.write('外資均價: ' + str(fi) + '\n')
                    
                    #畫圖
                    dfClean['收盤價'].plot()
                    plt.axhline(close25, color= 'r')
                    plt.axhline(close50, color= 'r')
                    plt.axhline(close75, color= 'r')
                    plt.axhline(closeMean, color= 'y')
                    plt.show()
                    
                    print('-------------------------------------------------------')
                    f.write('-------------------------------------------------------\n')
                    
            #except Exception:
                #pass
            
        f.close()
       
class str2:
    def __init__(self, ticker):
        self.ticker = ticker
        
    def strInout5d(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        row = dfInout.shape[0]
        cnt = 0
        
        for i in range(row):
            inout = dfInout.at[i, '外陸資買賣超股數(不含外資自營商)']
            inout = inout.replace(',','')
            
            if float(inout) <= 0:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinout5d'][self.ticker] = 'Y'
            
            dfTmp = gDfStock[gDfStock['股票代號'] == self.ticker]
            print(dfTmp)
            f.write(dfTmp.to_string(index = False) + '\n')
            
            print(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']])
            f.write(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤     
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
       
    def strInout5dAll(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        row = dfInout.shape[0]
        cnt = 0
        
        for i in range(row):
            inout = dfInout.at[i, '三大法人買賣超股數']
            inout = inout.replace(',','')
            
            if float(inout) <= 0:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinout5dall'][self.ticker] = 'Y'
            
            dfTmp = gDfStock[gDfStock['股票代號'] == self.ticker]
            print(dfTmp)
            f.write(dfTmp.to_string(index = False) + '\n')
            
            print(dfInout[['三大法人買賣超股數']])
            f.write(dfInout[['三大法人買賣超股數']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤       
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
        
    def strInout3d(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        row = dfInout.shape[0]
        cnt = 0
        
        if row > 3:
            row = 3
        
        for i in range(row):
            inout = dfInout.at[i, '外陸資買賣超股數(不含外資自營商)']
            inout = inout.replace(',','')
            
            if float(inout) <= 0:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinout3d'][self.ticker] = 'Y'
            tmp = gDfStock[gDfStock['股票代號'] == self.ticker]
            
            print(tmp)
            f.write(tmp.to_string(index = False) + '\n')
            
            print(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']])
            f.write(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤    
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
       
    def strInout3dAll(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        row = dfInout.shape[0]
        cnt = 0
        
        if row > 3:
            row = 3
        
        for i in range(row):
            inout = dfInout.at[i, '三大法人買賣超股數']
            inout = inout.replace(',','')
            
            if float(inout) <= 0:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinout3dall'][self.ticker] = 'Y'
            tmp = gDfStock[gDfStock['股票代號'] == self.ticker]
            
            print(tmp)
            f.write(tmp.to_string(index = False) + '\n')
            
            print(dfInout[['三大法人買賣超股數']])
            f.write(dfInout[['三大法人買賣超股數']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤  
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
        
    def strInoutDot5(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        dfPublish = gDfPublish[gDfPublish['代號'].str.strip() == str(self.ticker)]
        dfPublish = dfPublish.reset_index(drop = True)

        row = dfInout.shape[0]
        cnt = 0
        pc = 0.00
        
        if row > 1:
            row = 1
        
        for i in range(row):
            try:
                tdInout = dfInout.at[i, '外陸資買賣超股數(不含外資自營商)']
                tdInout = tdInout.replace(',','')
                
                ttInout = dfPublish.at[0, '上市股份']
                ttInout = ttInout.replace(',','')
                
                pc = float(tdInout) / (float(ttInout) * 1000)
                    
            except:
                print(str(self.ticker) + '無證券概況!')
                f.write(str(self.ticker) + '無證券概況!' + '\n')
                
            if pc <= 0.005:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinoutdot5'][self.ticker] = 'Y'
            
            tmpDf = gDfStock[gDfStock['股票代號'] == self.ticker]
            print(tmpDf)
            f.write(tmpDf.to_string(index = False) + '\n')
            
            print(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']])
            f.write(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤      
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            print('%: ' + str(pc))
            f.write('%: ' + str(pc) + '\n')
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
             
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
          
    def strInoutDot5All(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        dfPublish = gDfPublish[gDfPublish['代號'].str.strip() == str(self.ticker)]
        dfPublish = dfPublish.reset_index(drop = True)

        row = dfInout.shape[0]
        cnt = 0
        pc = 0.00
        
        if row > 1:
            row = 1
        
        for i in range(row):
            try:
                tdInout = dfInout.at[i, '三大法人買賣超股數']
                tdInout = tdInout.replace(',','')
                
                ttInout = dfPublish.at[0, '上市股份']
                ttInout = ttInout.replace(',','')
                
                pc = float(tdInout) / (float(ttInout) * 1000)
                    
            except:
                print(str(self.ticker) + '無證券概況!')
                f.write(str(self.ticker) + '無證券概況!' + '\n')
                
            if pc <= 0.005:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinoutdot5all'][self.ticker] = 'Y'
            
            tmpDf = gDfStock[gDfStock['股票代號'] == self.ticker]
            print(tmpDf)
            f.write(tmpDf.to_string(index = False) + '\n')
            
            print(dfInout[['三大法人買賣超股數']])
            f.write(dfInout[['三大法人買賣超股數']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤   
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            
            print('%: ' + str(pc))
            f.write('%: ' + str(pc) + '\n')
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
            
    def strInoutDot3(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        dfPublish = gDfPublish[gDfPublish['代號'].str.strip() == str(self.ticker)]
        dfPublish = dfPublish.reset_index(drop = True)
        
        row = dfInout.shape[0]
        cnt = 0
        pc = 0.00
        
        if row > 1:
            row = 1
        
        for i in range(row):
            try:
                tdInout = dfInout.at[i, '外陸資買賣超股數(不含外資自營商)']
                tdInout = tdInout.replace(',','')
                
                ttInout = dfPublish.at[0, '上市股份']
                ttInout = ttInout.replace(',','')
                pc = float(tdInout) / (float(ttInout) * 1000)
                
            except:
                print(str(self.ticker) + '無證券概況!')
                f.write(str(self.ticker) + '無證券概況!' + '\n')
                
            if pc <= 0.003:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinoutdot3'][self.ticker] = 'Y'
            
            tmpDf = gDfStock[gDfStock['股票代號'] == self.ticker]
            print(tmpDf)
            f.write(tmpDf.to_string(index = False) + '\n')
            
            print(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']])
            f.write(dfInout[['外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)', '外陸資買賣超股數(不含外資自營商)']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤      
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            print('%: ' + str(pc))
            f.write('%: ' + str(pc) + '\n')
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
        
    def strInoutDot3All(self):
        dfInout = gDfInout[gDfInout['證券代號'] == str(self.ticker)]
        dfInout = dfInout.reset_index(drop = True)
        
        dfPublish = gDfPublish[gDfPublish['代號'].str.strip() == str(self.ticker)]
        dfPublish = dfPublish.reset_index(drop = True)
        
        row = dfInout.shape[0]
        cnt = 0
        pc = 0.00
        
        if row > 1:
            row = 1
        
        for i in range(row):
            try:
                tdInout = dfInout.at[i, '三大法人買賣超股數']
                tdInout = tdInout.replace(',','')
                
                ttInout = dfPublish.at[0, '上市股份']
                ttInout = ttInout.replace(',','')
                pc = float(tdInout) / (float(ttInout) * 1000)
                
            except:
                print(str(self.ticker) + '無證券概況!')
                f.write(str(self.ticker) + '無證券概況!' + '\n')
                
            if pc <= 0.003:
                break
            
            cnt = cnt + 1
            
        if cnt >= row:
            gDfStats['stinoutdot3all'][self.ticker] = 'Y'
            
            tmpDf = gDfStock[gDfStock['股票代號'] == self.ticker]
            print(tmpDf)
            f.write(tmpDf.to_string(index = False) + '\n')
            
            print(dfInout[['三大法人買賣超股數']])
            f.write(dfInout[['三大法人買賣超股數']].to_string(index = False) + '\n')
            
            dfDesc = pd.read_csv('desc\\' + str(self.ticker) + '.TW_DESC.csv') 
            close25 = dfDesc.at[4, '收盤價']   #取25%關盤  
            close50 = dfDesc.at[5, '收盤價']   #取50%關盤 
            
            dfClean = pd.read_csv('clean\\' + str(self.ticker) + '.TW_CLEAN.csv')
            row1 = dfClean.shape[0]
            close = dfClean.at[row1 - 1, '收盤價']
            
            print('%: ' + str(pc))
            f.write('%: ' + str(pc) + '\n')
            
            print('今日收盤: ' + str(close))
            f.write('今日收盤: ' + str(close) + '\n')
            
            print('25%: ' + str(close25))
            f.write('25%: ' + str(close25) + '\n')
            print('50%: ' + str(close50))
            f.write('50%: ' + str(close50) + '\n')
            
            dfFi = pd.read_csv('fi\\' + str(self.ticker) + '.TW_FI_DESC.csv')
            fi = dfFi.at[1, 'Temp']/dfFi.at[1, 'fiin']
            print('外資均價: ' + str(fi))
            f.write('外資均價: ' + str(fi) + '\n')
            
            #畫圖
            dfClean['收盤價'].plot()
            
            close75 = dfDesc.at[6, '收盤價']   #取50%關盤
            closeMean = dfDesc.at[1, '收盤價']   #取50%關盤
            
            plt.axhline(close25, color= 'r')
            plt.axhline(close50, color= 'r')
            plt.axhline(close75, color= 'r')
            plt.axhline(closeMean, color= 'y')
            plt.show()
            
            print('-------------------------------------------------------')
            f.write('-------------------------------------------------------\n')
        
def craw_inout():
    dfInout = pd.DataFrame()
    cnt = 0
    
    for i in range(21):
        try:
            if cnt >= 5:
                break
            
            gdate = datetime.date.today() - datetime.timedelta(days = i)
            print(str(gdate))
            
            timeStruct = time.strptime(str(gdate), "%Y-%m-%d")
            strTime = time.strftime("%Y%m%d", timeStruct)
            dfTmp = get_inout(strTime)
            dfInout = pd.concat([dfInout, dfTmp], ignore_index = True)
            
            if not dfTmp.empty:
                cnt = cnt + 1
                
        except Exception:
            pass
    
    dfInout.to_csv(r'D:\python\stock\inout\inout_5d.csv')
    return dfInout 

def get_inout(date):
    try:
        dfInout = pd.DataFrame()
        dfInout = pd.read_csv('D:\python\stock\inout\\' + date + '.csv')
    
    except Exception:
        pass
    
    return dfInout        

#發送測試郵件
def sendTestMail(grp):
	#第三方 SMTP 服务
    mailUser = "wnrrcn@gmail.com"    #用户名
    mailPass = ""   #口令 

    sender = 'no-reply@gmail.com'
    receivers = ['joshshen1025@gmail.com', 'box922927@yahoo.com.tw', 'afra7318@hotmail.com']
    #receivers = ['box922927@yahoo.com.tw']

	#创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = Header("自動報表", 'utf-8')
    message['To'] = Header("所有收件者", 'utf-8')
    subject = '測試郵件，統計' + grp
    message['Subject'] = Header(subject, 'utf-8')
	 
	#邮件正文内容file:///D:/python/stock/stock_st_inout0.3%25.py
    message.attach(MIMEText('這是一封測試郵件..', 'plain', 'utf-8'))
	 
	# 构造附件2，传送当前目录下的 runoob.txt 文件
    att1 = MIMEText(open('stock_25%_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1["Content-Disposition"] = 'attachment;filename="stock_25%_' + grp + '.txt"'
    message.attach(att1)
    
    att2 = MIMEText(open('stock_50%_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att2["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att2["Content-Disposition"] = 'attachment;filename="stock_50%_' + grp + '.txt"'
    message.attach(att2)
    
    att3 = MIMEText(open('stock_5d_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att3["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att3["Content-Disposition"] = 'attachment;filename="stock_5d_' + grp + '.txt"'
    message.attach(att3)
    
    att4 = MIMEText(open('stock_jump_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att4["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att4["Content-Disposition"] = 'attachment;filename="stock_jump_' + grp + '.txt"'
    message.attach(att4)
    
    att5 = MIMEText(open('stock_inout5d_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att5["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att5["Content-Disposition"] = 'attachment;filename="stock_inout5d_' + grp + '.txt"'
    message.attach(att5)
    
    att6 = MIMEText(open('stock_inout5dall_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att6["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att6["Content-Disposition"] = 'attachment;filename="stock_inout5dall_' + grp + '.txt"'
    message.attach(att6)
    
    att7 = MIMEText(open('stock_inout3d_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att7["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att7["Content-Disposition"] = 'attachment;filename="stock_inout3d_' + grp + '.txt"'
    message.attach(att7)
    
    att8 = MIMEText(open('stock_inout3dall_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att8["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att8["Content-Disposition"] = 'attachment;filename="stock_inout3dall_' + grp + '.txt"'
    message.attach(att8)
    
    att9 = MIMEText(open('stock_inout0.5%_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att9["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att9["Content-Disposition"] = 'attachment;filename="stock_inout0.5%_' + grp + '.txt"'
    message.attach(att9) 
    
    att10 = MIMEText(open('stock_inout0.5%all_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att10["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att10["Content-Disposition"] = 'attachment;filename="stock_inout0.5%all_' + grp + '.txt"'
    message.attach(att10)
    
    att11 = MIMEText(open('stock_inout0.3%_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att11["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att11["Content-Disposition"] = 'attachment;filename="stock_inout0.3%_' + grp + '.txt"'
    message.attach(att11)
    
    att12 = MIMEText(open('stock_inout0.3%all_' + grp + '.txt', 'rb').read(), 'base64', 'utf-8')
    att12["Content-Type"] = 'application/octet-stream'
	# 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att12["Content-Disposition"] = 'attachment;filename="stock_inout0.3%all_' + grp + '.txt"'
    message.attach(att12)
    
    att13 = MIMEText(open(grp + '_stats.csv', 'rb').read(), 'base64', 'utf-8')
    att13["Content-Type"] = 'application/octet-stream'
    att13["Content-Disposition"] = 'attachment; filename="' + grp + '_stats.csv"'
    message.attach(att13)

    try:
        smtpObj = smtplib.SMTP("smtp.gmail.com:587")# 587 为 SMTP 端口号
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.login(mailUser, mailPass)  
        smtpObj.sendmail(sender, receivers, message.as_string())
        
        print("郵件發送成功")

    except smtplib.SMTPException:
        print("無法發送郵件!")

gDfInout = craw_inout() 

gListGrp = ['stock_50', 'stock_100', 'stock_etf', 'stock_add']

for g in gListGrp:
    gDfStock = pd.read_csv(g + '.csv')     
    gTickers = list(gDfStock.loc[:, '股票代號'])  
    
    s = str1(gTickers, g)
    
    gDfPublish = pd.read_csv('publish\chk202003.csv') 
    
    gDfStats = pd.read_csv(g + '_stats.csv')
    gDfStats.set_index("股票代號", inplace = True)
    gDfStats.loc[:, 'lt25'] = ''
    
    #策略: 25%
    s.strUnder25()
    
    #os.system("pause");
    
    gDfStats.loc[:, 'lt50'] = ''
    
    #策略: 50%
    s.strUnder50()
    
    #os.system("pause");
    
    gDfStats.loc[:, 'st5d'] = ''
    
    #策略: 突破5日高點
    s.str5dHigher()
    
    #os.system("pause");
    
    gDfStats.loc[:, 'stjp'] = ''
    
    #策略: 跳空
    s.strJump()
    
    #os.system("pause");
    
    #策略: 外資買超5days
    print('=======================================================')
    print('策略: 外資買超5days')
    print('=======================================================')
    
    f = open('stock_inout5d_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 外資買超5days\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinout5d'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInout5d()
        
    f.close()
    
    #os.system("pause");
    
    #策略: 三大法人買超5days
    print('=======================================================')
    print('策略: 三大法人買超5days')
    print('=======================================================')
    
    f = open('stock_inout5dall_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 三大法人買超5days\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinout5dall'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInout5dAll()
        
    f.close()
    
    #os.system("pause");
    
    #策略: 外資買超3days
    print('=======================================================')
    print('策略: 外資買超3days')
    print('=======================================================')
    
    f = open('stock_inout3d_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 外資買超3days\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinout3d'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInout3d()

    f.close()
    
    #os.system("pause");
    
    #策略: 三大法人買超3days
    print('=======================================================')
    print('策略: 三大法人買超3days')
    print('=======================================================')
    
    f = open('stock_inout3dall_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 三大人買超3days\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinout3dall'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInout3dAll()

    f.close()
    
    #os.system("pause");
    
    #策略: 外資買超0.5%
    print('=======================================================')
    print('策略: 外資買超0.5%')
    print('=======================================================')
    
    f = open('stock_inout0.5%_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 外資買超0.5%\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinoutdot5'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInoutDot5()
    
    f.close() 
    
    #os.system("pause");
    
    #策略: 三大法人買超0.5%
    print('=======================================================')
    print('策略: 三大法人買超0.5%')
    print('=======================================================')
    
    f = open('stock_inout0.5%all_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 三大法人買超0.5%\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinoutdot5all'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInoutDot5All()
    
    f.close() 
    
    #os.system("pause");
    
    print('=======================================================')
    print('策略: 外資買超0.3%')
    print('=======================================================')
    
    f = open('stock_inout0.3%_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 外資買超0.3%\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinoutdot3'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInoutDot3()
    
    f.close() 
    
    #os.system("pause");
    
    print('=======================================================')
    print('策略: 三大法人買超0.3%')
    print('=======================================================')
    
    f = open('stock_inout0.3%all_' + g + '.txt','w')
    f.write('=======================================================\n')
    f.write('策略: 三大法人買超0.3%\n')
    f.write('=======================================================\n')
    
    gDfStats.loc[:, 'stinoutdot3all'] = ''
    
    for i in gTickers:
        s1 = str2(i)
        
        s1.strInoutDot3All()
    
    f.close() 
    
    #os.system("pause");
    
    gDfStats.to_csv(r'D:\python\stock\\' + g + '_stats.csv')
    
    sendTestMail(g)