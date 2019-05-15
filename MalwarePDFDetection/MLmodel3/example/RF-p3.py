#!/usr/bin/python
# -*- coding: utf-8 -*-

import xlrd
import csv
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.interpolate import spline

# 设置路径
path = '/home/yonah/God_with_me/2018Q1/MLmodel2/example/merge_con.csv'
# 读取文件
df = pd.read_csv(path, header=0)
print (df)
# df.info()

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

    feat_id = X.columns.tolist()
    XX = np.array(X)
    Xint = XX.astype(int)




    return y,Xint.tolist(),feat_id


# 训练随机森林模型
from sklearn.cross_validation import train_test_split
from sklearn.ensemble import RandomForestClassifier

y,X ,f_id= data_clear(df)
x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
feat_labels = df.columns[1:]
forest = RandomForestClassifier(n_estimators=10000, random_state=0, n_jobs=-1)
forest.fit(x_train, y_train)

# 打印特征重要性评分
importances = forest.feature_importances_
# indices = np.argsort(importances)[::-1]
imp = []
for f in range(x_train.shape[1]):
    print(f + 1, feat_labels[f], importances[f])

# 将打印的重要性评分copy到featureScore.xlsx中；plot特征重要性
# 设置路径
path = '/home/yonah/God_with_me/2018Q1/MLmodel2/example/featureScore.xlsx'
# 打开文件
myBook = xlrd.open_workbook(path)
# 查询工作表
sheet_1_by_index = myBook.sheet_by_index(0)
data = []
for i in range(0, sheet_1_by_index.nrows):
    data.append(sheet_1_by_index.row_values(i))
data = np.array(data)
X = data[:1, ].ravel()
y = data[1:, ]
plt.figure(1, figsize=(8, 4))
i = 0
print(len(y))
while i < len(y):
    # power_smooth = spline(X,y[i],xnew)
    # plt.grid(True)
    plt.legend(loc='best')
    plt.plot(X, y[i], linewidth=1)
    plt.ylabel('Log(1/R)')
    plt.xlabel('wavelength(nm)')
    i = i + 1
plt.legend(loc='best')
plt.savefig('/home/yonah/God_with_me/2018Q1/MLmodel2/example/featureScore', dpi=200)
plt.show()