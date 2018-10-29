#!/usr/bin/python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from sklearn import decomposition
from sklearn.datasets import load_svmlight_file
from mpl_toolkits.mplot3d import Axes3D

from sklearn import preprocessing
import numpy as np

min_max_scaler = preprocessing.MinMaxScaler()



filename = "/home/yonah/God_with_me/pdf-t/test_data/hidost2K.libsvm"
data = load_svmlight_file(filename)
feature, label = data[0], data[1]
feat = feature.todense()
list_label = label.tolist()
list_feat = feat.tolist()

# jiangwei:dimensionality reduction
pca = decomposition.PCA(n_components=3)
newX = pca.fit_transform(list_feat)
print newX
newX = min_max_scaler.fit_transform(newX)  # normalization

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')


from pandas import DataFrame
data = DataFrame(newX,columns=list('a''b''c'))
data['label'] = list_label


x_values = data.loc[data['label']==1].a.tolist()
y_values = data.loc[data['label']==1].b.tolist()
z_values = data.loc[data['label']==1].c.tolist()
#ax.scatter(x_values, y_values, z_values,c='r')
#ax.scatter(x_values,x_values,x_values,c='r')

'''
x_values = data.loc[data['label']==0].a.tolist()
y_values = data.loc[data['label']==0].b.tolist()
z_values = data.loc[data['label']==0].c.tolist()
ax.scatter(x_values, y_values, z_values,c='b')
'''
x_values = data.loc[data['label']==0].a.tolist()
ax.scatter(x_values,y_values,z_values,c='r')

ax.set_xlabel('X Label')
ax.set_ylabel('Y Label')
ax.set_zlabel('Z Label')

plt.show()