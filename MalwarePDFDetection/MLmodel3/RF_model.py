#!/usr/bin/python
# -*- coding: utf-8 -*-

import xlrd
import csv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import spline

# 设置路径
path = '/home/yonah/God_with_me/2018Q1/MLmodel2/example/merge_con.csv'
# 读取文件
df = pd.read_csv(path, header=0)

# df.info()

# 训练随机森林模型
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestClassifier

x, y = df.iloc[:, 1:].values, df.iloc[:, 0].values
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=0)
feat_labels = df.columns[1:]
forest = RandomForestClassifier(n_estimators=10000, random_state=0, n_jobs=-1)
forest.fit(x_train, y_train)

# 打印特征重要性评分
importances = forest.feature_importances_
# indices = np.argsort(importances)[::-1]
imp = []
for f in range(x_train.shape[1]):
    print(f + 1, feat_labels[f], importances[f])

# 将打印的重要性评分copy到featureScore.xlsx中；plot特征重要性
# 设置路径
path = '/home/yonah/God_with_me/2018Q1/MLmodel2/example/featureScore.xlsx'
# 打开文件
myBook = xlrd.open_workbook(path)
# 查询工作表
sheet_1_by_index = myBook.sheet_by_index(0)
data = []
for i in range(0, sheet_1_by_index.nrows):
    data.append(sheet_1_by_index.row_values(i))
data = np.array(data)
X = data[:1, ].ravel()
y = data[1:, ]
plt.figure(1, figsize=(8, 4))
i = 0
print(len(y))
while i < len(y):
    # power_smooth = spline(X,y[i],xnew)
    # plt.grid(True)
    plt.legend(loc='best')
    plt.plot(X, y[i], linewidth=1)
    plt.ylabel('Log(1/R)')
    plt.xlabel('wavelength(nm)')
    i = i + 1
plt.legend(loc='best')
plt.savefig('/home/yonah/God_with_me/2018Q1/MLmodel2/example/featureScore', dpi=200)
plt.show()