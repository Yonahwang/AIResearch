#!/usr/bin/python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from sklearn import decomposition
from sklearn.datasets import load_svmlight_file
import pandas


filename = "/home/yonah/God_with_me/pdf-t/test_data/hidost2K.libsvm"
data = load_svmlight_file(filename)
feature, label = data[0], data[1]
feat = feature.todense()    #to dense
list_label = label.tolist()
list_feat = feat.tolist()

# jiangwei:dimensionality reduction
pca = decomposition.PCA(n_components=2)
#data_xy = pca.fit(list_feat)
newX=pca.fit_transform(list_feat)
print newX

import numpy as np
from pandas import DataFrame
data = DataFrame(newX,columns=list('a''b'))
data['label'] = list_label

#print data.head(10)
x_values = data.loc[data['label']==0].a.tolist()
y_values = data.loc[data['label']==0].b.tolist()

x_values = data.loc[data['label']==1].a.tolist()
y_values = data.loc[data['label']==1].b.tolist()



'''
scatter() 
x:横坐标 y:纵坐标 s:点的尺寸
'''


plt.scatter(x_values, y_values, c = 'g',marker = '.')



# 设置图表标题并给坐标轴加上标签
plt.title('File distribution', fontsize=24)
plt.xlabel('Value', fontsize=14)
plt.ylabel('Square of Value', fontsize=14)


# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)
plt.show()