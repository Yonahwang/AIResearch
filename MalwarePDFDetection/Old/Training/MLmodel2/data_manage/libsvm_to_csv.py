#!/usr/bin/python
# -*- coding: utf-8 -*-

from sklearn.datasets import dump_svmlight_file
import pandas as pd
import numpy as np


df = pd.read_csv("transfered.csv")

y_data = np.array(df['is_malware'])
del df['is_malware']
del df['sha1']

X_data = np.array(df)

dump_svmlight_file(X_data,y_data,'apk_libsvm.dat',zero_based=False,multilabel=False)

