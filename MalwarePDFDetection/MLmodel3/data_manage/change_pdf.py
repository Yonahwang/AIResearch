#!/usr/bin/python
# -*- coding: utf-8 -*-

import os

for file in os.listdir('/home/yonah/Data/Mal2k'):
    if file[-4: ] != '.pdf':
        new_name = file + '.pdf'
        os.rename('/home/yonah/Data/Mal2k/'+ file, '/home/yonah/Data/Mal2k/' + new_name)
