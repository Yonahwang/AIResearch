#!/usr/bin/python
# -*- coding: utf-8 -*-



'''
    from python.svmutil import *
    m = svm_train(train_y, train_x)  #训练
    svm_predict(test_y, test_x, m)   #predict
    y, x = svm_read_problem('train.1.txt')  # 读入训练数据
    yt, xt = svm_read_problem('test.1.txt')  # 训练测试数据
    m = svm_train(y, x)  # 训练
    svm_predict(yt, xt, m)  # 测试'''


from sklearn.svm import SVC

import matplotlib.pyplot as plt

import numpy as np

X=np.array([[1,1],[1,2],[1,3],[1,4],[2,1],[2,2],[3,1],[4,1],[5,1],

       [5,2],[6,1],[6,2],[6,3],[6,4],[3,3],[3,4],[3,5],[4,3],[4,4],[4,5]])

Y=np.array([1]*14+[-1]*6)

T=np.array([[0.5,0.5],[1.5,1.5],[3.5,3.5],[4,5.5]])

svc=SVC(kernel='poly',degree=2,gamma=1,coef0=0)

svc.fit(X,Y)

pre=svc.predict(T)

print pre

print svc.n_support_

print svc.support_

print svc.support_vectors_