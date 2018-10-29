#!/usr/bin/python
# -*- coding: utf-8 -*-


import os

#file_root = r"/home/yonah/PDFdata/pdfnormal/"
file_root = r"/home/yonah/PDFdata/filevirus-noParse/"
pathDir = os.listdir(file_root)

file_list = []
fi = open("/home/yonah/God_with_me/hidost/mpdfs.txt",'a')
for file in pathDir:
    file_com = file_root + file
    #file_list.append(file_com)
    fi.write(file_com + "\n")

#fi.writelines(file_list)
fi.close()