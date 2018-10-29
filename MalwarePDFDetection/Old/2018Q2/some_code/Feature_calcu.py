#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np


ben_tarin1 = pd.read_csv('/home/yonah/Downloads/mimicus-master/data/contagio.csv')
mal_tarin1 = pd.read_csv('/home/yonah/Downloads/mimicus-master/data/contagio-mal.csv')


def data_clear(file):
    file['class'] = file['class'].astype(bool)
    file['class'] = file['class'].astype(int)
    y = file['class'].tolist()

    #print y
    X = file[['count_acroform', 'count_acroform_obs', 'count_action', 'count_action_obs', 'count_box_a4', 'count_box_legal', 'count_box_letter',
                'count_box_other', 'count_box_overlap', 'count_endobj', 'count_endstream', 'count_eof', 'count_font',
                'count_font_obs', 'count_image_large', 'count_image_med', 'count_image_small', 'count_image_total',
                'count_image_xlarge', 'count_image_xsmall', 'count_javascript', 'count_javascript_obs', 'count_js',
                'count_js_obs', 'count_obj', 'count_objstm', 'count_objstm_obs', 'count_page', 'count_page_obs',
                'count_startxref', 'count_stream', 'count_stream_diff', 'count_trailer', 'count_xref','creator_len','keywords_len', 'len_obj_avg', 'len_obj_max',
                'len_obj_min', 'len_stream_avg', 'len_stream_max', 'len_stream_min', 'pdfid0_len',
                 'pdfid1_len','producer_len', 'subject_len','title_len',]]

    feat_id = X.columns.tolist()
    return y, X, feat_id



_, fea ,fea_id = data_clear(mal_tarin1)
ave = np.mean(fea, axis=0)  # axis=0，计算每一列的均值
print ave









