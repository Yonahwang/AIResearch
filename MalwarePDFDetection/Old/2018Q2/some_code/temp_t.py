#!/usr/bin/python
# -*- coding: utf-8 -*-

from pandas import *
import pandas as pd

from random import *
name1 = [1,2,3]
test1 = [2,3,4]
p1 = [3,4,5]
p2 = [4,5,6]
ac = [0,0,0.99,0.97]
dfac = DataFrame(columns=('name', 'leble', 'RF','KNN' ))#生成空的pandas表
dfac.loc[0] = ac



df1 = DataFrame(columns=('name', 'leble', 'RF','KNN' ))#生成空的pandas表
df1['name'] = name1
df1['leble'] = test1
df1 ['RF'] = p1
df1['KNN'] = p2

below = df1.loc[0:]
newdf = pd.concat([dfac,below],ignore_index=True)
print newdf

print '********* spilt line ***********************'
print df1

print '********* spilt line ***********************'
df = DataFrame({'name':name1,
                    'leble':test1,
                    'RF':p1,
                    'KNN':p2
})

print df





