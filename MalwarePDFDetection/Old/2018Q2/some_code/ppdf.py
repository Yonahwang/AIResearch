#!/usr/bin/python
# -*- coding: utf-8 -*-


import os

for file in os.listdir('/home/yonah/PDFdata/mal'):
    if file[-4: ] != '.pdf':
        new_name = file + '.pdf'
        os.rename('/home/yonah/PDFdata/mal/'+ file, '/home/yonah/PDFdata/mal/' + new_name)
