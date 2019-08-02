# -*- coding: UTF-8 -*-
import csv
import time
import requests
import re
URL="http://202.104.69.205:8089/EnvServiceForFoshanTest/EnvCriteriaAqiServiceForCities.svc"
HEADER={
'Accept': '*/*',
'Accept-Language': 'zh-CN',
'Referer': 'http://202.104.69.205:8089/FOAQIPublish/ClientBin/Glass.xap',
'Content-Length': '188',
'Accept-Encoding': 'identity',
'Content-Type': 'text/xml; charset=utf-8',
'SOAPAction': "http://tempuri.org/IEnvCriteriaAqiServiceForCities/GetCityAQIPublish",
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; rv:11.0) like Gecko',
'Host': '202.104.69.205:8089',
'Connection': 'Keep-Alive',
'Pragma': 'no-cache'
}
xml="""<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><GetCityAQIPublish xmlns="http://tempuri.org/"><city>佛山,佛山2</city></GetCityAQIPublish></s:Body></s:Envelope>"""




def getdata():
    r = requests.post(URL,headers=HEADER,data=xml)
    return r.text
def parsdata(textdata):
    # 数据定义，预处理
    soup=textdata.split("<a:Tab_AQIPublishInfo>")
    pattern1 = re.compile(r'(</.*?>)')
    pattern2= re.compile(r'(<.*?>)')
    data=[]
    date=time.strftime('%Y-%m-%d',time.localtime(time.time()))
    filename='./tianqi'+date+'.csv'

    # 数据解析生成list
    for i in range(1,len(soup)):
        row=re.sub(pattern2, '',re.sub(pattern1, ',',soup[i]))
        data.append(row.split(',')[:-2])
    print(len(data[1]))
    name_attribute = ['AQI','CO','CO_24h','ID','Latitude','Longitude','NO2','NO2_24h','O3','O3_1h','O3_8h','O3_8h_24h','PM10','PM10_24h','PM2_5','PM2_5_24h','PositionName','PrimaryPollutant','Quality','SO2','SO2_24h','StationCode','TimePoint','Unheathful']
    print(len(name_attribute))

    # 数据输出
    csvFile = open(filename, "w+")
    try:
        writer = csv.writer(csvFile)
        writer.writerow(name_attribute)
        for i in range(len(data)):
            writer.writerow(data[i])
    finally:
        csvFile.close()

if __name__ == '__main__':
    textdata=getdata()
    parsdata(textdata)