#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import numpy as np
import pandas as pd
from sklearn import decomposition
import matplotlib.pyplot as plt
from sklearn import datasets, linear_model
from sklearn import metrics
import pickle
import random

f_tarin = pd.read_csv('/home/yonah/God_with_me/2018Q1/MLmodel2/example/merge_con.csv')


def data_clear(file):
    #label = file[['class']]

    a = file['class']=='FALSE'
    b = file['class'] == 'TRUE'
    file.loc[a,'class'] = False
    file.loc[b, 'class'] = True


    file['class'] = file['class'].astype(bool)
    file['class'] = file['class'].astype(int)
    #print file['class']
    #  print file.loc[file['class']==0]   #  print x ==0 lines
    y = file['class'].tolist()

    '''for i in np.array(label).tolist():
        if i == 'Ture':
            y.append(1)
        else:
            y.append(0)'''

    #print y
    X = file[['author_dot', 'author_lc', 'author_len', 'author_mismatch', 'author_num', 'author_oth', 'author_uc',
                'box_nonother_types', 'box_other_only', 'company_mismatch', 'count_acroform', 'count_acroform_obs',
                'count_action', 'count_action_obs', 'count_box_a4', 'count_box_legal', 'count_box_letter',
                'count_box_other', 'count_box_overlap', 'count_endobj', 'count_endstream', 'count_eof', 'count_font',
                'count_font_obs', 'count_image_large', 'count_image_med', 'count_image_small', 'count_image_total',
                'count_image_xlarge', 'count_image_xsmall', 'count_javascript', 'count_javascript_obs', 'count_js',
                'count_js_obs', 'count_obj', 'count_objstm', 'count_objstm_obs', 'count_page', 'count_page_obs',
                'count_startxref', 'count_stream', 'count_stream_diff', 'count_trailer', 'count_xref', 'createdate_dot',
                'createdate_mismatch', 'createdate_ts', 'createdate_tz', 'createdate_version_ratio', 'creator_dot',
                'creator_lc', 'creator_len', 'creator_mismatch', 'creator_num', 'creator_oth', 'creator_uc', 'delta_ts',
                'delta_tz', 'image_mismatch', 'image_totalpx', 'keywords_dot', 'keywords_lc', 'keywords_len',
                'keywords_mismatch', 'keywords_num', 'keywords_oth', 'keywords_uc', 'len_obj_avg', 'len_obj_max',
                'len_obj_min', 'len_stream_avg', 'len_stream_max', 'len_stream_min', 'moddate_dot', 'moddate_mismatch',
                'moddate_ts', 'moddate_tz', 'moddate_version_ratio', 'pdfid0_dot', 'pdfid0_lc', 'pdfid0_len',
                'pdfid0_mismatch', 'pdfid0_num', 'pdfid0_oth', 'pdfid0_uc', 'pdfid1_dot', 'pdfid1_lc', 'pdfid1_len',
                'pdfid1_mismatch', 'pdfid1_num', 'pdfid1_oth', 'pdfid1_uc', 'pdfid_mismatch', 'pos_acroform_avg',
                'pos_acroform_max', 'pos_acroform_min', 'pos_box_avg', 'pos_box_max', 'pos_box_min', 'pos_eof_avg',
                'pos_eof_max', 'pos_eof_min', 'pos_image_avg', 'pos_image_max', 'pos_image_min', 'pos_page_avg',
                'pos_page_max', 'pos_page_min', 'producer_dot', 'producer_lc', 'producer_len', 'producer_mismatch',
                'producer_num', 'producer_oth', 'producer_uc', 'ratio_imagepx_size', 'ratio_size_obj',
                'ratio_size_page', 'ratio_size_stream', 'size', 'subject_dot', 'subject_lc', 'subject_len',
                'subject_mismatch', 'subject_num', 'subject_oth', 'subject_uc', 'title_dot', 'title_lc', 'title_len',
                'title_mismatch', 'title_num', 'title_oth', 'title_uc', 'version']]

    XX = np.array(X)
    Xint = XX.astype(int)



    return y,Xint.tolist()

label,feature = data_clear(f_tarin)

pca = decomposition.PCA(n_components=2)
newX=pca.fit_transform(feature)

from sklearn import preprocessing
min_max_scaler = preprocessing.MinMaxScaler()
newX = min_max_scaler.fit_transform(newX)

from pandas import DataFrame
data = DataFrame(newX,columns=list('a''b'))
data['label'] = label

x_values = data.loc[data['label']==0].a.tolist()
y_values = data.loc[data['label']==0].b.tolist()

x1_values = data.loc[data['label']==1].a.tolist()
y1_values = data.loc[data['label']==1].b.tolist()


plt.scatter(x_values, y_values, c = 'g',marker = '.')
plt.scatter(x1_values, y1_values, c = 'r',marker = '^')
# 设置图表标题并给坐标轴加上标签
plt.title('File distribution', fontsize=24)
plt.xlabel('Value', fontsize=14)
plt.ylabel('Square of Value', fontsize=14)


# 设置刻度标记的大小
plt.tick_params(axis='both', which='major', labelsize=14)
plt.show()