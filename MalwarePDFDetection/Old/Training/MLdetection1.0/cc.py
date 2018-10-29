# -*- coding: utf-8 -*-#!/usr/bin/python
import sys
import shutil
import pickle
import gzip
import glob
from pyspark import SparkContext,SparkConf

PESaveFileRoot = '/home/deyuan'#目标存储目录

# TODO: Instead of using the local spark, use the Bluedon Spark Cluster instead
conf = SparkConf().setAppName('test').setMaster('local[4]')
sc = SparkContext(conf=conf)
sc.addPyFile('/usr/local/lib/python2.7/dist-packages/pefile-2017.5.26-py2.7.egg')#导入依赖库
#import pefile

def MoveMalwareFile(spath, dpath): #移动
    try:
        shutil.move(spath, dpath)
    except Exception:
        print('moving file meet error')

def CopyMalwareFile(spath, dfiledir):  #复制
    try:
        shutil.copy(spath, dfiledir)
    except Exception:
        print('copy file meet error')

def PEclassfyD(content): #content为二进制文件流
    result= False
    if(b'MZ' in content[:2]):
        print('one')
        try:
            f = pefile.PE(data=content)
            f.close()
            #MoveMalwareFile(filepath,PESaveFileRoot) #移动文件
            #CopyMalwareFile(filepath,PESaveFileRoot)  #复制文件
            result =True
        except Exception:
            result = False
    return  result

def read_fun_generator(filename):
    with gzip.open(filename, 'rb') as f:
        if PEclassfyD(f.read()):
            return True
    return False

#主函数
import os
if not os.path.exists(PESaveFileRoot):
    os.makedirs(PESaveFileRoot)
import os

gz_filelist = list()
# TODO: Instead of reading from local file folder, read the files from the Bluedon HDFS
databaselist = ['/home/zhuoxiong/DELETE.2017-08-15','/home/zhuoxiong/DELETE.2017-08-16','/home/zhuoxiong/DELETE.2017-08-17','/home/zhuoxiong/DELETE.2017-08-18']
for i in range(len(databaselist)):
    for dirpath, dirname, filenames in os.walk(databaselist[i] + '/samples'):
        for filename in filenames:
            rfpath = os.path.join(dirpath, filename)
            gz_filelist.append(rfpath)

print('total file is %d'%len(gz_filelist))
#PEfilePathCollect = []
#i = 0
#for fp in gz_filelist:
#   if read_fun_generator(fp):
#	i+=1
#	if i%1000 == 0:
#		print('the %d file'%i)
#	PEfilePathCollect.append(fp)
PEfilePathCollect = sc.parallelize(gz_filelist).filter(read_fun_generator).collect()  #spark method

# TODO: ADD the ML model and perform the prediction operation
#循环遍历所有的文件
#添加代码
print('pe file is %d'%len(PEfilePathCollect))
pickle.dump(PEfilePathCollect,open(PESaveFileRoot+'/mal2017_9_26.pkl','wb'))
sc.stop()
sys.exit()
