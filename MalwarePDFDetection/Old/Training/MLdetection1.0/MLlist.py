# -*- coding: utf-8 -*-
# !/usr/bin/python

# 导入需要用到的库
import os
import numpy as np
import datetime
import pickle
from feature import *


# 全局参数配置，根据需要自己修改以下六个参数
Benign_File_Root = r"/Users/fengjiaowang/Downloads/data2000/pdf" # 正常样本数据集的文件路径
Melicious_File_Root = r"/Users/fengjiaowang/Downloads/data2000/VirusS" # 恶意样本数据集的文件路径
#Benign_File_Root = r"/home/yonah/PDFdata/pdfnormal" # 正常样本数据集的文件路径
#Melicious_File_Root = r"/home/yonah/PDFdata/malPDF" # 恶意样本数据集的文件路径


# 载入数据集
def load_file(file_Path):
    test_files = list()
    for dirpath, dirname, filenames in os.walk(file_Path):
        for filename in filenames:
            rfpath = os.path.join(dirpath, filename)
            test_files.append(rfpath)
    return test_files


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

def data_get(norfile_path_list,malfile_path_list):
    file_feature = []
    file_class = []
    alldata_path = norfile_path_list + malfile_path_list
    for froot in alldata_path:
        try:
            if froot in norfile_path_list:
                cla = 0
            else:
                cla = 1
            #****************************************************************
            fe_list = featureCheckRead(froot, cla)
            if not fe_list:
                pdf = fakeFile_check(froot)
                if pdf:
                    file_class.append(cla)
                    fe_list, fe_key = feature_extract(pdf)
                    file_feature.append(fe_list)  # 对输入文件进行特征提取
                    featureCheckUpdate(froot, cla, fe_list)  # 更新特征集
            else:
                file_class.append(cla)
                file_feature.append(fe_list)  # 对输入文件进行特征提取

        except Exception as e:
            print('file %s feature extracting meet ERROR' % froot)
            print(e)
            continue

    return file_feature,file_class
bfiles = load_file(Benign_File_Root)  # 载入正常样本文件路径
mfiles = load_file(Melicious_File_Root)  # 载入恶意文件路径
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



    # get file

    #train_x = train_feature ,train_y = train_class, test_x = test_feature
    feature_x,class_y= data_get(bfiles,mfiles)

    print('normdict len is %d,maldict len is %d'%(len(maldict),len(nordict)))
    pickle.dump(nordict, open('normdictfile.pl', 'wb'))
    pickle.dump(maldict, open('maldictfile.pl', 'wb'))

    # make libsvm
    from sklearn.datasets import dump_svmlight_file
    dump_svmlight_file(feature_x, class_y, 'pdf_feature.libsvm', zero_based=False, multilabel=False)

    print('DONE')

if __name__ == '__main__':
    start = datetime.datetime.now()
    main()

    end = datetime.datetime.now()
    print "spend time = %d s" % (end - start).seconds