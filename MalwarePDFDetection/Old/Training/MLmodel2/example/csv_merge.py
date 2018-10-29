#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import glob

def hebing():
    csv_list = []
    csv_list.append('/home/yonah/Downloads/mimicus-master/data/contagio-mal.csv')
    #csv_list.append('/home/yonah/Downloads/mimicus-master/data/virustotal-mal.csv')
    csv_list.append('/home/yonah/Downloads/mimicus-master/data/contagio.csv')
    #csv_list.append('/home/yonah/Downloads/mimicus-master/data/google-ben.csv')

    print(u'共发现%s个CSV文件'% len(csv_list))
    print(u'正在处理............')
    for i in csv_list:
        fr = open(i,'r').read()
        with open('merge_con.csv','a') as f:
            f.write(fr)
    print(u'合并完毕！')

def quchong(file):
    df = pd.read_csv(file,header=0)
    datalist = df.drop_duplicates()
    datalist.to_csv(file)

if __name__ == '__main__':
    hebing()
    quchong("merge_con.csv")

