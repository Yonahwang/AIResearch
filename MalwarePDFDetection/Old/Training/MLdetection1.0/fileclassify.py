# -*- coding: utf-8 -*-
# !/usr/bin/python

# 导入需要用到的库

import os
from peepdf.PDFCore import PDFParser
import shutil



# 全局参数配置，根据需要自己修改以下六个参数
#file_root = r"/Users/fengjiaowang/Downloads/data2000/pdf/" # 正常样本数据集的文件路径
file_root = r"/Users/fengjiaowang/Downloads/data2000/VirusS/"


def get_data(f_list):
    for i in f_list:
        T_file = file_root + i
        pdfParser = PDFParser()

        try:
            _, pdf = pdfParser.parse(T_file)

        except Exception:
            os.remove(file_root+i)
            # shutil.move(file_root+i, file_classify+i)   #复制文件到新文件夹  ，移动用move/copyfile
            print i
            continue

    return i


def  get_count(path):
    count = 0
    for root, dirs, files in os.walk(path):  # 遍历统计
        for each in files:
            count += 1  # 统计文件夹下文件个数
    print count  # 输出结果


if __name__ == '__main__':
    print('start processing')
    #file_classify = '/home/yonah/PDFdata/malpdf/'
    #if not os.path.exists(file_classify):  # 判断文件夹是否存在
        #os.mkdir('/home/yonah/PDFdata/malpdf')

    file_list = os.listdir(file_root)  # 列出文档
    get_data(file_list)
    print ("DONE")


