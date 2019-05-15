#!/usr/bin/python
# -*- coding: utf-8 -*-


from sklearn.datasets import load_svmlight_file
import random
import datetime
from sklearn import metrics


filename = "/home/yonah/God_with_me/pdf-t/test_data/hidost2K.libsvm"
data = load_svmlight_file(filename)
feature, label = data[0], data[1]
feat = feature.todense()
list_label = label.tolist()
list_feat = feat.tolist()
#print list_label


#select data to train
# 数据采集随机化，避免过拟合

def data_Ran(fe,la):
    test_all = []
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(fe, la, test_size=0.2)
    return X_train, X_test, y_train, y_test,test_all


# 分析识别率
def predect_calcu(predict, test_y, binary_class=True):
    if binary_class:
        precision = metrics.precision_score(test_y, predict)
        recall = metrics.recall_score(test_y, predict)

        confusion = metrics.confusion_matrix(test_y, predict)
        TP = confusion[1, 1]
        TN = confusion[0, 0]
        FP = confusion[0, 1]
        FN = confusion[1, 0]

        print("TP:%d *** TN:%d *** FP:%d *** FN:%d " % (TP, TN, FP, FN))
        print('precision: %.2f%%, recall: %.2f%%' % (100 * precision, 100 * recall))
    accuracy = metrics.accuracy_score(test_y, predict)
    print('accuracy: %.2f%%' % (100 * accuracy))
    return accuracy, confusion

def SVM_SVC(Strain_x, Stest_x,Strain_y,Stest_y):
    print('*********SVM_SVC processing************************')
    print('#train data: %d, dimension: %d' % (len(Strain_x), len(Strain_x[0])))
    from sklearn.svm import SVC
    svc = SVC(kernel='poly', degree=2, gamma=1, coef0=0)
    svc.fit(Strain_x, Strain_y)
    pre = svc.predict(Stest_x)
    list_pre = pre.tolist()
    predect_calcu(list_pre, Stest_y)

# 定义多层感知机分类算法
def neural_network(Ntrain_x, Ntest_x,Ntrain_y,Ntest_y):
    from sklearn.neural_network import MLPClassifier
    NNet = MLPClassifier(activation='relu', solver='adam', alpha=0.0001)
    print('*********neural_network processing************************')
    print('#train data: %d, dimension: %d' % (len(Ntrain_x), len(Ntrain_x[0])))
    NNet.fit(Ntrain_x, Ntrain_y)
    pre = NNet.predict(Ntest_x)
    list_pre = pre.tolist()
    predect_calcu(list_pre, Ntest_y)

    """参数
    ---
        hidden_layer_sizes: 元祖
        activation：激活函数
        solver ：优化算法{‘lbfgs’, ‘sgd’, ‘adam’}
        alpha：L2惩罚(正则化项)参数。
    """

if __name__ == '__main__':
    start = datetime.datetime.now()
    print('*********start processing************************')

    from sklearn.model_selection import train_test_split
    train_x, test_x,train_y,test_y = train_test_split(list_feat, list_label, test_size=0.2)
    #SVM_SVC(train_x, test_x,train_y,test_y)
    neural_network(train_x, test_x,train_y,test_y)

    end = datetime.datetime.now()
    print "spend time = %d s" % (end - start).seconds


