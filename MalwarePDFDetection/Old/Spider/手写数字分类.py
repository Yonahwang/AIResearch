# -*- coding: utf-8 -*-
"""
Created on Fri Sep  1 17:28:51 2017

@author: Administrator
"""

from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC 

'''
    由于灰度值为0-16，为了简单表示：
    灰度值为7以下的设置为0(不够黑直接判为0)
    灰度值大于7的设置为1(够黑判为1)
'''
def change(X):
    # 小于8的像素点为0
    X[X<8]=0
    
    # 大于7的像素点为1
    X[X>7]=1
    return X

'''
    load_digits()有1083个手写数字（0，1，2，3，4，5）样本
    每一个样本由8*8的4bit像素（0，16）灰度图片组成
    因此特征的维数为64，每一个像素为1个特征
'''

# 读取数据
digits = datasets.load_digits(n_class=6)
X = digits.data
y = digits.target

# 将 像素点 转换为 0或1
X = change(X)

# 将数据分成 训练组(0.8) 和 测试组(0.2)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# 调用 SVC 并且 传递训练组数据
clf = SVC()
clf.fit(X_train,y_train)

# 输出 训练组 和 测试组的 准确率
print(clf.score(X_train,y_train))
print(clf.score(X_test,y_test))

# 输出 报告
from sklearn.metrics import classification_report
print(classification_report(y_test, clf.predict(X_test)))