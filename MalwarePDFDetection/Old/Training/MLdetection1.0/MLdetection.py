# -*- coding: utf-8 -*-
# !/usr/bin/python

# 导入需要用到的库
#form __futurn __ import division
import os
import random
from sklearn import metrics
import matplotlib.pyplot as plt
import numpy as np
import datetime
#import simple_plot as sp
import pickle
from feature import *
import multiprocessing

# 全局参数配置，根据需要自己修改以下六个参数
Benign_File_Root = r"/Users/fengjiaowang/Downloads/data2000/pdf" # 正常样本数据集的文件路径
Melicious_File_Root = r"/Users/fengjiaowang/Downloads/data2000/VirusS" # 恶意样本数据集的文件路径
#Benign_File_Root = r"/home/yonah/PDFdata/pdfnormal" # 正常样本数据集的文件路径
#Melicious_File_Root = r"/home/yonah/PDFdata/malPDF" # 恶意样本数据集的文


Benign_File_For_Trainning =50  # 用于训练的正常样本的个数
Melicious_File_For_Trainning =50  # 用于训练的恶意样本的个数
Benign_File_For_Test =50  # 用于测试的正常样本的个数
Melicious_File_For_Test =50  # 用于测试的恶意样本的个数


# Random Forest Classifier
def random_forest_classifier(train_x, train_y):
    from sklearn.ensemble import RandomForestClassifier
    model = RandomForestClassifier(n_estimators=200)  # 根据自己的需要，指定随机森林的树的个数
    model.fit(train_x, train_y)
    return model


# 载入数据集
def load_file(PEfile_Path):
    test_files = list()
    for dirpath, dirname, filenames in os.walk(PEfile_Path):
        for filename in filenames:
            rfpath = os.path.join(dirpath, filename)
            test_files.append(rfpath)
    return test_files


# 数据采集随机化，避免过拟合
def datebase_divide(*arg):
    import math
    if len(arg) == 2:
        DatabaseTrainNum_Normal = int(math.floor(arg[0] / 2))
        DatabaseTestNum_normal = arg[0] - DatabaseTrainNum_Normal
        DatabaseTrainNum_Melidious = int(math.floor(arg[1] / 2))
        DatabaseTestNum_Melidious = arg[1] - DatabaseTrainNum_Normal
    else:
        DatabaseTrainNum_Normal = arg[0]
        DatabaseTestNum_normal = arg[1]
        DatabaseTrainNum_Melidious = arg[2]
        DatabaseTestNum_Melidious = arg[3]
    templist = [i for i in range(DatabaseTrainNum_Normal + DatabaseTestNum_normal)]
    trainlist_normal = random.sample(templist, DatabaseTrainNum_Normal)  # 选择正常的训练样本
    testlist_normal = [i for i in templist if i not in trainlist_normal]

    templist = [i + DatabaseTrainNum_Normal + DatabaseTestNum_normal for i in
                range(DatabaseTrainNum_Melidious + DatabaseTestNum_Melidious)]
    trainlist_melicious = random.sample(templist, DatabaseTrainNum_Melidious)
    testlist_melicious = [i for i in templist if i not in trainlist_melicious]

    trainlist = trainlist_normal + trainlist_melicious
    random.shuffle(trainlist)  # 训练样本随机性

    testlist = testlist_normal + testlist_melicious
    return trainlist, testlist


def featureCheckRead(froot,label = 0):
    global nordict,maldict
    if label == 0:
        tempdict = nordict
    else:
        tempdict = maldict
    key = froot.split('/')[-1]
    if key in tempdict:
        return tempdict[key]
    else:
        return None

def featureCheckUpdate(froot ,label,feature):
    global nordict,maldict
    if label == 0:
        tempdict = nordict
    else:
        tempdict = maldict
    key = froot.split('/')[-1]
    tempdict[key] = feature


# 数据处理与特征提取，，此处需重点修改
def data_get(trainSampleMark, testSampleMark):
    global mfiles,bfiles,DatabaseTotalNums_Normal,benignrandom,malrandom
    btotal = DatabaseTotalNums_Normal
    gcroot_normal = bfiles
    gcroot_melicious = mfiles
    train_feature = []
    train_class = []
    test_feature = []
    test_class = []
    tename = []
    fe_key = []     #feature_key


    print("normal sample number is %d" % len(gcroot_normal))
    print("malware sample number is %d" % len(gcroot_melicious))
    print('begin to read the dataset')


    for i in trainSampleMark:
        try:
            cla = 0
            if i<btotal:
                froot = gcroot_normal[benignrandom[i]]
            else:
                froot=gcroot_melicious[malrandom[i - btotal]]
                cla = 1
            #*******************************************************************
            fe_list = featureCheckRead(froot,cla)
            if not fe_list:
                pdf = fakeFile_check(froot)
                if pdf:
                    train_class.append(cla)
                    fe_list,fe_key = feature_extract(pdf)
                    train_feature.append(fe_list) #对输入文件进行特征提取
                    featureCheckUpdate(froot,cla,fe_list)  #更新特征集
            else:
                train_class.append(cla)
                train_feature.append(fe_list)  # 对输入文件进行特征提取

        except Exception as e:
            print('file %s feature extracting meet ERROR'%froot)
            print(e)
            continue

    for i in testSampleMark:
        try:
            cla = 0
            if i<btotal:
                froot = gcroot_normal[benignrandom[i]]
            else:
                froot=gcroot_melicious[malrandom[i - btotal]]
                cla = 1


            # *******************************************************************
            fe_list = featureCheckRead(froot,cla)
            fileReadEnabel = True
            if not fe_list:
                pdf = fakeFile_check(froot)
                if pdf:
                    test_class.append(cla)
                    fe_list,fe_key= feature_extract(pdf)
                    test_feature.append(fe_list) #对输入文件进行特征提取
                    featureCheckUpdate(froot,cla,fe_list)  #更新特征集
                else:
                    fileReadEnabel = False
            else:
                print('have readed')
                test_class.append(cla)
                test_feature.append(fe_list)  # 对输入文件进行特征提取
            if fileReadEnabel:
                tname = froot.split('/')[-1]
                tename.append(tname)

        except Exception as e:
            print('file %s feature extracting meet ERROR' % froot)
            print(e)
            continue

    return train_feature, train_class, test_feature, test_class,tename,fe_key


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

def tabledemo(name1,test1,predict1):
    name = []
    test = []
    predict= []
    for i in range(len(predict1)):
        if test1[i] != predict1[i]:
            name.append(name1[i])
            test.append(test1[i])
            predict.append(predict1[i])
    table = {'name': name, 'test':test , 'predint': predict}
    import pandas
    frame = pandas.DataFrame(table)
    return frame

def plot_importance(f_v,fe_id):
    f_id = fe_id
    #f_id = [str(i) for i in range(len(f_v))]
    f_v = 100.0 * (f_v / f_v.max())

    f_id = np.array(f_id)

    sorted_idx = np.argsort(-f_v)

    pos = np.arange(len(sorted_idx)) + 0.5
    plt.subplot(1, 2, 2)
    plt.title('Feature Importance')

    plt.barh(pos[0:30], f_v[sorted_idx][0:30], color='r', align='center')
    plt.yticks(pos[0:30], f_id[sorted_idx][0:30])
    plt.xlabel('Relative Importance')
    plt.draw()
    plt.show()



DatabaseTotalNums_Normal = Benign_File_For_Trainning + Benign_File_For_Test  # 所有正常样本的个数
DatabaseTotalNums_mal = Melicious_File_For_Trainning + Melicious_File_For_Test #all Mal test document

bfiles = load_file(Benign_File_Root)  # 载入正常样本文件路径
benignrandom = random.sample([i for i in range(len(bfiles))],DatabaseTotalNums_Normal)
mfiles = load_file(Melicious_File_Root)  # 载入恶意文件路径
malrandom = random.sample([i for i in range(len(mfiles))],DatabaseTotalNums_mal)
nordict = {}
maldict = {}

def main():
    print('start processing')
    global  nordict,maldict

    if True:
        if os.path.exists('normdictfile.pl'):
            nordict = pickle.load(open('normdictfile.pl','rb'))
        if os.path.exists('maldictfile.pl'):
            maldict = pickle.load(open('maldictfile.pl','rb'))



    trainSampleMark, testSampleMark = datebase_divide(Benign_File_For_Trainning,
                                                      Benign_File_For_Test,
                                                      Melicious_File_For_Trainning,
                                                      Melicious_File_For_Test)
    #train_x = train_feature ,train_y = train_class, test_x = test_feature
    train_x, train_y, test_x, test_y,tena,f_id= data_get(trainSampleMark, testSampleMark)

    print('normdict len is %d,maldict len is %d'%(len(maldict),len(nordict)))
    pickle.dump(nordict, open('normdictfile.pl', 'wb'))
    pickle.dump(maldict, open('maldictfile.pl', 'wb'))

    # make libsvm
    feature_x = train_x + test_x
    class_y = train_y + test_y
    from sklearn.datasets import dump_svmlight_file
    dump_svmlight_file(feature_x, class_y, 'libsvmby1.dat', zero_based=False, multilabel=False)
    start = datetime.datetime.now()
    print('******************** Train Data Info *********************')
    print('#train data: %d, dimension: %d' % (len(train_x), len(train_x[0])))
    clf = random_forest_classifier(train_x, train_y)
    plot_importance(clf.feature_importances_,f_id)
    #RF_Information_print(clf)
    predict, predictp = ml_predict(clf, test_x)
    #print'predint : ',list(predict)
    predect_calcu(predict, test_y)
    #print'test_y : ',test_y
    print('******************** flie analysis *********************')
    print tabledemo(tena,test_y,predict)
    end = datetime.datetime.now()
    print "spend time detction = %d s" % (end - start).seconds
    print('DONE')

if __name__ == '__main__':
    start = datetime.datetime.now()
    main()

    end = datetime.datetime.now()
    print "spend time = %d s" % (end - start).seconds