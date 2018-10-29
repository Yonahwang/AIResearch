# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 11:36:58 2017

@author: Guobin Huang

功能：将大量文件同一命名

"""

import os
import os.path
import hashlib

# 为文件增加后缀.apk
def rename_add_text(filepath):
    filedir,filename = os.path.split(filepath)
    return filename+'.apk'

# 用文件的sha1值命名
def rename_sha1(filepath):
    f = open(filepath,'rb')
    sha1obj = hashlib.sha1()
    sha1obj.update(f.read())
    file_sha1 = sha1obj.hexdigest()
    f.close()
    
    filedir,filename = os.path.split(filepath)
    filetext = os.path.splitext(filepath)[1]
    new_filename = file_sha1+filetext
    
    if new_filename != filename and os.path.isfile(os.path.join(filedir,new_filename)): # 判断是否存在相同的文件
        print("Warning...存在相同的文件")
        return 'W_'+new_filename
    return new_filename

# 遍历文件的函数
def change_file_name(filedir,refunc):
    print("Use "+refunc.__name__+"() to change file name")
    for parent,dirnames,filenames in os.walk(filedir):
        for filename in filenames:
            old_filepath = os.path.join(parent,filename)
            new_filename = refunc(old_filepath)
            new_filepath = os.path.join(parent,new_filename)
            os.rename(old_filepath,new_filepath)
            print("Rename success：" + old_filepath + " --> " + new_filename)
            
change_file_name('..\\cpDrebin\\sample\\apk',rename_sha1)