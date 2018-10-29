#!/usr/bin/python
# -*- coding: utf-8 -*-

# 导入需要用到的库
import os
import numpy as np
import datetime
import pickle
from sklearn.datasets import load_svmlight_file
from sklearn import metrics
import numpy as np

#input file_feature
filename = "/home/yonah/God_with_me/pdf-t/test_data/hidost2K.libsvm"
data = load_svmlight_file(filename)
feature, label = data[0], data[1]
feat = feature.todense()
list_label = label.tolist()
list_feat = feat.tolist()


# Random Forest Classifier
def random_forest_classifier(train_x, train_y):
    from sklearn.ensemble import RandomForestClassifier
    model = RandomForestClassifier(n_estimators=200)  # 根据自己的需要，指定随机森林的树的个数
    model.fit(train_x, train_y)
    return model


# 对测试数据分类
def ml_predict(clf, test_x):
    print('******************** Test Data Info *********************')
    print('#testing data: %d, dimension: %d' % (len(test_x), len(test_x[0])))
    if clf:
        predictp = clf.predict_proba(test_x)
        predict = clf.predict(test_x)
        return predict, predictp

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

def main():
    print('start processing')


    #train_x = train_feature ,train_y = train_class, test_x = test_feature
    from sklearn.model_selection import train_test_split
    train_x, test_x, train_y, test_y = train_test_split(list_feat, list_label, test_size=0.2)

    print('******************** RF Train Data Info *********************')
    print('#train data: %d, dimension: %d' % (len(train_x), len(train_x[0])))
    clf = random_forest_classifier(train_x, train_y)
    predict, predictp = ml_predict(clf, test_x)
    #print'predint : ',list(predict)
    predect_calcu(predict, test_y)
    #print'test_y : ',test_y
    print('******************** flie analysis *********************')
    end = datetime.datetime.now()
    print "spend time detction = %d s" % (end - start).seconds
    # 保存模型
    with open('model1.0.pickle', 'wb') as f:
        pickle.dump(clf, f)
    print('DONE')

if __name__ == '__main__':
    start = datetime.datetime.now()
    main()

    end = datetime.datetime.now()
    print "spend time = %d s" % (end - start).seconds