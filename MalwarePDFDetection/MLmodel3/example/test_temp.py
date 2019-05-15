#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from pandas import DataFrame


name_list = ['author_dot', 'author_lc', 'author_len', 'author_mismatch', 'author_num']
num_list = [5.39079101e-04,1.15897698e-04,4.84011775e-05,3.51138912e-04,3.85006090e-04]
dic = {'feat':name_list,'imp':num_list}
data = DataFrame(dic)

newI = data.sort_values(by='imp',axis=0,ascending=False)
newI.index = [i for i in range(len(newI))]
print newI
x = newI['feat'].tolist()
y = newI['imp'].tolist()

#num_list = 100.0 * (num_list / num_list.max())
#f_id = np.array(name_list)
#sorted_idx = np.argsort(-num_list)
#pos = np.arange(len(sorted_idx)) + 0.5


#plt.bar(pos[0:30], name_list[sorted_idx][0:30], color='g', tick_label=name_list)
plt.barh(range(len(x)), y, color='g', tick_label=x)
ax = plt.gca()
#set(gca, 'XTick', 1:50, 'XTickLabel', app_name,'FontSize',14)
#set(gca,'XTickLabelRotation',90)
plt.subplots_adjust(left=0.2, bottom=0.3, right=0.9, top=0.8,hspace=0.2,wspace=0.3)
plt.setp( ax.xaxis.get_majorticklabels(), rotation=-45 )
plt.show()



