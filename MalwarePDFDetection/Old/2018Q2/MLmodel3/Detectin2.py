#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import pickle

from ML3 import data_clear

f_test = pd.read_csv('/home/yonah/Downloads/mimicus-master/mimicus/bin/F_gdkse.csv')

y,X ,f_id= data_clear(f_test)


# 读取模型
with open('model_csv2.0.pickle', 'rb') as f:
    model = pickle.load(f)
model.predict(X)