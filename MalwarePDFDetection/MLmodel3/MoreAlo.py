#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import numpy as np
import pandas as pd
import datetime
from sklearn import datasets, linear_model
from sklearn import metrics
import pickle
import matplotlib.pyplot as plt
import random
from pandas import DataFrame



#f_tarin = pd.read_csv('/home/yonah/God_with_me/2018Q2/MLmodel3/example/merge_real.csv') # 10K samples, balanced dataset
f_tarin = pd.read_csv('/home/yonah/God_with_me/2018Q2/MLmodel3/example/test4K.csv')

def data_clear(file):

    #a = file['class']=='FALSE'
    #b = file['class'] == 'TRUE'
    #file.loc[a,'class'] = False
    #file.loc[b, 'class'] = True


    file['class'] = file['class'].astype(bool)
    file['class'] = file['class'].astype(int)
    #print file['class']
    #  print file.loc[file['class']==0]   #  print x ==0 lines
    y = file[['class','filename']]
    NY = np.array(y)
    NYY = NY.tolist()

    #fiName = file['filename'].tolist()

    '''for i in np.array(label).tolist():
        if i == 'Ture':
            y.append(1)
        else:
            y.append(0)'''

    #print y
    X = file[['author_dot', 'author_lc', 'author_len', 'author_mismatch', 'author_num', 'author_oth', 'author_uc',
                'box_nonother_types', 'box_other_only', 'company_mismatch', 'count_acroform', 'count_acroform_obs',
                'count_action', 'count_action_obs', 'count_box_a4', 'count_box_legal', 'count_box_letter',
                'count_box_other', 'count_box_overlap', 'count_endobj', 'count_endstream', 'count_eof', 'count_font',
                'count_font_obs', 'count_image_large', 'count_image_med', 'count_image_small', 'count_image_total',
                'count_image_xlarge', 'count_image_xsmall', 'count_javascript', 'count_javascript_obs', 'count_js',
                'count_js_obs', 'count_obj', 'count_objstm', 'count_objstm_obs', 'count_page', 'count_page_obs',
                'count_startxref', 'count_stream', 'count_stream_diff', 'count_trailer', 'count_xref', 'createdate_dot',
                'createdate_mismatch', 'createdate_ts', 'createdate_tz', 'createdate_version_ratio', 'creator_dot',
                'creator_lc', 'creator_len', 'creator_mismatch', 'creator_num', 'creator_oth', 'creator_uc', 'delta_ts',
                'delta_tz', 'image_mismatch', 'image_totalpx', 'keywords_dot', 'keywords_lc', 'keywords_len',
                'keywords_mismatch', 'keywords_num', 'keywords_oth', 'keywords_uc', 'len_obj_avg', 'len_obj_max',
                'len_obj_min', 'len_stream_avg', 'len_stream_max', 'len_stream_min', 'moddate_dot', 'moddate_mismatch',
                'moddate_ts', 'moddate_tz', 'moddate_version_ratio', 'pdfid0_dot', 'pdfid0_lc', 'pdfid0_len',
                'pdfid0_mismatch', 'pdfid0_num', 'pdfid0_oth', 'pdfid0_uc', 'pdfid1_dot', 'pdfid1_lc', 'pdfid1_len',
                'pdfid1_mismatch', 'pdfid1_num', 'pdfid1_oth', 'pdfid1_uc', 'pdfid_mismatch', 'pos_acroform_avg',
                'pos_acroform_max', 'pos_acroform_min', 'pos_box_avg', 'pos_box_max', 'pos_box_min', 'pos_eof_avg',
                'pos_eof_max', 'pos_eof_min', 'pos_image_avg', 'pos_image_max', 'pos_image_min', 'pos_page_avg',
                'pos_page_max', 'pos_page_min', 'producer_dot', 'producer_lc', 'producer_len', 'producer_mismatch',
                'producer_num', 'producer_oth', 'producer_uc', 'ratio_imagepx_size', 'ratio_size_obj',
                'ratio_size_page', 'ratio_size_stream', 'size', 'subject_dot', 'subject_lc', 'subject_len',
                'subject_mismatch', 'subject_num', 'subject_oth', 'subject_uc', 'title_dot', 'title_lc', 'title_len',
                'title_mismatch', 'title_num', 'title_oth', 'title_uc', 'version']]

    feat_id = X.columns.tolist()
    XX = np.array(X)
    Xint = XX.astype(int)
    return NYY,Xint.tolist()


def SVM(X_train,y_train,X_test,data_y):
    from sklearn.svm import SVC
    model = SVC(C=1.0)
    # 拟合模型
    model.fit(X_train, y_train)
    # 模型预测
    return model.predict(X_test), model.score(X_test, data_y)

def NNet(X_train,y_train,X_test,data_y):
    from sklearn.neural_network import MLPClassifier
    model = MLPClassifier(activation='relu', solver='adam', alpha=0.0001)
    # 拟合模型
    model.fit(X_train, y_train)
    # 模型预测
    return model.predict(X_test), model.score(X_test, data_y)

def KNN(X_train,y_train,X_test,data_y):
    from sklearn import neighbors
    model = neighbors.KNeighborsClassifier(n_neighbors=5, n_jobs=1)  # 分类
    #model = neighbors.KNeighborsRegressor(n_neighbors=5, n_jobs=1)  # 回归
    # 拟合模型
    model.fit(X_train, y_train)
    # 模型预测
    return model.predict(X_test), model.score(X_test, data_y)

def NB(X_train,y_train,X_test,data_y):
    from sklearn import naive_bayes
    #model = naive_bayes.GaussianNB()  # 高斯贝叶斯
    model = naive_bayes.MultinomialNB(alpha=1.0, fit_prior=True, class_prior=None)
    #model = naive_bayes.BernoulliNB(alpha=1.0, binarize=0.0, fit_prior=True, class_prior=None)
    # 拟合模型
    model.fit(X_train, y_train)
    # 模型预测
    return model.predict(X_test), model.score(X_test, data_y)

def RF(X_train,y_train,X_test,data_y):
    from sklearn.ensemble import RandomForestClassifier
    model = RandomForestClassifier(n_estimators=200)  # 根据自己的需要，指定随机森林的树的个数
    model.fit(X_train, y_train)
    # 拟合模型
    model.fit(X_train, y_train)
    # 模型预测
    return model.predict(X_test), model.score(X_test, data_y)

def AnalysisTofile(name1,test1,p1,p2,p3,p4,ac):
    df = DataFrame(columns=('name', 'leble', 'RF','KNN','NNET','SVM'))  # 生成空的pandas表
    df['name'] = name1
    df['leble'] = test1
    df['RF'] = p1
    df['KNN'] = p2
    df['NNET'] = p3
    df['SVM'] = p4

    dfac = DataFrame(columns=('name', 'leble', 'RF', 'KNN','NNET','SVM'))  # 生成空的pandas表
    a = [0,0]
    for i in ac:
        a.append(int(100*i))

    dfac.loc[0] = a

    below = df.loc[0:]
    newdf = pd.concat([dfac, below], ignore_index=True)
    return newdf.head(10)


def ySpilt(list):
    DF_y = pd.DataFrame(list, columns=['Y', 'finame'])
    DF = DF_y['Y']
    DF2 = DF_y['finame']
    y = DF.tolist()
    finame = DF2.tolist()
    return y,finame


def main():
    print('start processing')

    y, X = data_clear(f_tarin)

    from sklearn.model_selection import train_test_split
    train_x, test_x, train_Y, test_Y = train_test_split(X, y, test_size=0.2, random_state=1,)  # randomize samples
    train_y, _ = ySpilt(train_Y)
    test_y,fina = ySpilt(test_Y)



    print('********************Train Data Info *********************')
    print('#train data: %d, dimension: %d' % (len(train_x), len(train_x[0])))
    FileOne1, ac1 = RF(train_x, train_y,test_x,test_y)
    #FileOne2, ac2 = NB(train_x, train_y, test_x, test_y)
    FileOne3, ac3 = KNN(train_x, train_y, test_x, test_y)
    FileOne4, ac4 = NNet(train_x, train_y, test_x, test_y)
    FileOne5, ac5 = SVM (train_x, train_y, test_x, test_y)

    Li = [ac1,ac3,ac4,ac5]

    print AnalysisTofile(fina, test_y, FileOne1,FileOne3,FileOne4,FileOne5,Li)




    #print('******************** flie analysis *********************')
    #print AnalysisTofile(tena, test_y, predict)
    end = datetime.datetime.now()
    print "spend time detction = %d s" % (end - start).seconds
    print('DONE')


if __name__ == '__main__':
    start = datetime.datetime.now()
    main()

    end = datetime.datetime.now()
    print "spend time = %d s" % (end - start).seconds