# -*- coding: utf-8 -*-
# !/usr/bin/python



# 全局参数配置，根据需要自己修改以下六个参数
Benign_File_Root = r"/Users/fengjiaowang/Downloads/data2000/pdf" # 正常样本数据集的文件路径
Melicious_File_Root = r"/Users/fengjiaowang/Downloads/data2000/VirusS" # 恶意样本数据集的文件路径
#Benign_File_Root = r"/home/yonah/PDFdata/pdfnormal" # 正常样本数据集的文件路径
#Melicious_File_Root = r"/home/yonah/PDFdata/malPDF" # 恶意样本数据集的文


import os
import random
import datetime

Benign_File_For_Trainning =10  # 用于训练的正常样本的个数
Melicious_File_For_Trainning =20  # 用于训练的恶意样本的个数
Benign_File_For_Test =10  # 用于测试的正常样本的个数
Melicious_File_For_Test =10  # 用于测试的恶意样本的个数




def fakeFile_check(filePath):
    try:
        from peepdf.PDFCore import PDFParser
        pdfParser = PDFParser()
        _, pdf = pdfParser.parse(filePath)
        return pdf
    except Exception:
        return None

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

# 数据处理与特征提取，，此处需重点修改
def file_get(gcroot_normal, gcroot_melicious, trainSampleMark, testSampleMark, btotal):

    train_class = []
    train_name = []
    test_name = []
    test_class = []

    print("normal sample number is %d" % len(gcroot_normal))
    print("malware sample number is %d" % len(gcroot_melicious))
    print('begin to read the dataset')

    for i in trainSampleMark:
        try:
            cla = 0
            if i < btotal:
                froot = gcroot_normal[i]
            else:
                froot = gcroot_melicious[i - btotal]
            cla = 1
            # *******************************************************************
            pdf = fakeFile_check(froot)
            if pdf:
                train_class.append(cla)
                train_pash = froot
                train_name.append(train_pash)

        except Exception:
            print('file %s feature extracting meet ERROR' % froot)
            continue

    for i in testSampleMark:
        try:
            cla = 0
            if i < btotal:
                froot = gcroot_normal[i]
            else:
                froot = gcroot_melicious[i - btotal]
                cla = 1

                # *******************************************************************
            pdf = fakeFile_check(froot)
            if pdf:
                test_pash = froot
                test_name.append(test_pash)
        except Exception:
            print('file %s feature extracting meet ERROR' % froot)
            continue

    return  train_name, test_name

def file_selection():
    print('start processing')
    trainSampleMark, testSampleMark = datebase_divide(Benign_File_For_Trainning,
                                                      Benign_File_For_Test,
                                                      Melicious_File_For_Trainning,
                                                      Melicious_File_For_Test)

    bfiles = load_file(Benign_File_Root)  # 载入正常样本文件路径
    mfiles = load_file(Melicious_File_Root)  # 载入恶意文件路径
    DatabaseTotalNums_Normal = Benign_File_For_Trainning + Benign_File_For_Test  # 所有正常样本的个数
    train_name, test_name = file_get(bfiles, mfiles, trainSampleMark, testSampleMark, DatabaseTotalNums_Normal)


    return train_name, test_name

if __name__ == '__main__':
    start = datetime.datetime.now()
    train_name, test_name = file_selection()
    print train_name
    print test_name
    end = datetime.datetime.now()

    print start
    print end
    print "spent time = %d s " %(end - start).seconds