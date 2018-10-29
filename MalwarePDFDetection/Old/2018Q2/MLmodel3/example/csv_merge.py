#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import glob

def hebing():
    csv_list = []

    csv_list.append('/home/yonah/God_with_me/2018Q2/data-set/normal_sogpi2K.csv')  # 2K
    # fb = pd.read_csv('/home/yonah/God_with_me/tempp/ben1w.csv',sep=None)
    csv_list.append('/home/yonah/God_with_me/2018Q2/data-set/c9k.csv')
    # csv_list.append('/home/yonah/Downloads/mimicus-master/data/contagio.csv') #5K
    # csv_list.append('/home/yonah/Downloads/mimicus-master/data/google-ben.csv') #5K
    #csv_list.append('/home/yonah/Downloads/mimicus-master/mimicus/bin/contogio_ben9K.csv')  # 9K
    #csv_list.append('/home/yonah/Downloads/mimicus-master/mimicus/bin/normal_sogpi2K.csv')  # 2K
    # csv_list.append('/home/yonah/God_with_me/2018Q2/data-set/Virus8K.csv') #8K
    #csv_list.append('/home/yonah/God_with_me/2018Q2/data-set/Virus-noParse.csv')  # 8K

    print(u'共发现%s个CSV文件'% len(csv_list))
    print(u'正在处理............')
    for i in csv_list:
        fr = open(i,'r').read()
        with open('merge_ben1w.csv','a') as f:
            f.write(fr)
    print(u'合并完毕！')

def quchong(file):

    df = pd.read_csv(file,header=0)

    datalist = df.drop_duplicates()
    datalist.to_csv(file)

if __name__ == '__main__':
    hebing()
    quchong("merge_ben1w.csv")

