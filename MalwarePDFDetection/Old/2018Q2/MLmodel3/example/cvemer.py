#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import glob


mal_tarin1 = pd.read_csv('/home/yonah/God_with_me/2018Q1/MLmodel2/example/merge_con.csv')
f1 = pd.read_csv('/home/yonah/God_with_me/2018Q2/data-set/normal_sogpi2K.csv') # 2K
print f1.head(1)
#fb = pd.read_csv('/home/yonah/God_with_me/tempp/ben1w.csv',sep=None)
fx = pd.read_csv('/home/yonah/God_with_me/2018Q2/data-set/mal2k.csv')
print fx.head(2)
#f2 = pd.read_csv('/home/yonah/God_with_me/2018Q2/data-set/congagio9K.csv')  # 9K
#f3 = pd.read_csv('/home/yonah/God_with_me/2018Q2/data-set/Virus-noParse.csv')




def marge(fi1,fi2):
    print fi1.head(2)
    print fi1['class']
    print fi2.head(3)
    md = pd.concat([fi1,fi2])
    dfm = pd.DataFrame(md)
    file = open("test4K.csv","w")
    return dfm



if __name__ == '__main__':
    print('start processing')
    dfm = marge(f1,fx)
    dfm.to_csv("test4K.csv",index=False,sep=',')
