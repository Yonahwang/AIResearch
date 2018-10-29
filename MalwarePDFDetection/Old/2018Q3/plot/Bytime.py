#!/usr/bin/python
# -*- coding: utf-8 -*-


import matplotlib.pyplot as plt

num_list = ['1997','1998','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015','2016','2017']
valu = [0.52, 0.79, 0.94, 2.62 ,1.31 ,1.26 ,1.57 ,1.62 ,2.88 ,3.56 ,3.40 ,3.14 ,5.65 ,5.34 ,4.92 ,6.91 ,7.59 ,12.88 ,11.83 ,8.27 ,12.98 ]
#plt.bar(num_list,valu,0.4,color="green")
plt.bar(range(len(valu)),valu, 0.5, color='green',tick_label=num_list)
plt.show()
