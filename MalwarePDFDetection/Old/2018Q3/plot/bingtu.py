#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

values = [462,294,192,220,13,8,1,1,84,61]

#labels = [4,2,1,3,5,8,6,7,9,0]
labels = 'Execute Code','Overflow ','Denial of Service' ,'Memory Corruption','Gain Privilege','XSS','Http Response Splitting','CSRF','Bypass Something','Gain Information'





# 设置x，y轴刻度一致，这样饼图才能是圆的
plt.axes(aspect=1)  # set this , Figure is round, otherwise it is an ellipse
# autopct ，show percet
plt.pie(x=values, labels=labels, autopct='%3.1f %%',shadow=True,
        labeldistance=1.1, startangle=90,pctdistance=0.7)
plt.show()





