# -*- coding: utf-8 -*-
"""
Created on 2017-10-20 14:51

@author: Guobin Huang

用法:
    import simple_plot
    # 假设需要绘制1行2列的3d图

    # 特征1
    X_data1 = np.array([[1, 1, 0, 0], [0, 1, 1, 1], [0, 0, 0, 1], [1, 0, 1, 0]])
    y_data1 = np.array([1, 1, 0, 0])

    # 特征2
    X_data2 = np.array([[1, 1, 0, 0, 0], [0, 1, 1, 1, 1], [0, 0, 0, 1, 0], [1, 0, 1, 0, 1]])
    y_data2 = np.array([1, 1, 0, 0])

    # 组合成list
    X_datas = [X_data1, X_data2]
    y_datas = [y_data1, y_data2]

    # 调用接口 1行 2列 X_datas y_datas 需要绘制点的数量(每一类为 2个,一共4个)
    plt = get_plot_3d(1, 2, X_datas, y_datas, 2)
    plt.show()

"""

try:
    import numpy as np
except:
    print("Please install numpy...")
    exit(0)

try:
    import matplotlib.pyplot as plt
    from mpl_toolkits.mplot3d import axes3d, Axes3D
except:
    print("Please install matplotlib...")
    exit(0)

try:
    from sklearn.decomposition import PCA
except:
    print("Please install sklearn...")
    exit(0)

def fix_pca(X_data,n_components):
    pca = PCA(n_components=n_components,copy=False)
    X_data = pca.fit_transform(X_data)
    return X_data

'''
    rows: 子图的行数
    cols: 子图的列数
    X_datas: 每个图的特征的list
    y_datas: 每个图的分类结果的list
    point_size: 需要选取点的数量,默认每类200个点,一共绘制400个点
    titles: 子图的标题,默认为空
    默认使用pca算法降维
'''
def get_plot_2d(rows,cols,X_datas,y_datas,point_size=200,titles = ['']):

    subplot_len = rows*cols
    if len(X_datas) != subplot_len or len(y_datas) != subplot_len:
        print("Len of X_datas/y_datas is error...")
        return

    if len(titles) != subplot_len:
        titles = ['' for i in range(subplot_len)]

    for i in range(subplot_len):
        plt.subplot(rows, cols, i + 1)
        plt.title(titles[i])

        X_data = X_datas[i]
        y_data = y_datas[i]

        # pca降维
        X_data = fix_pca(X_data,2)

        # 分类
        X0 = X_data[y_data == 0,]
        X1 = X_data[y_data == 1,]

        # 选取需要显示的点
        X0 = X0[:point_size]
        X1 = X1[:point_size]

        # 绘图
        plt.scatter(X0[:,0], X0[:,1], c='blue', alpha=0.5)
        plt.scatter(X1[:,0], X1[:,1], c='red', alpha=0.5, marker='x')

        if i == int((rows-1)*cols+cols/2): # 最后一行的中间
            plt.legend(['benign', 'malware'], loc='center', bbox_to_anchor=(0.5, -0.15), ncol=2)

    return plt

def get_plot_3d(rows,cols,X_datas,y_datas,point_size=200,titles=['']):
    fig = plt.figure()

    subplot_len = rows*cols
    if len(X_datas) != subplot_len or len(y_datas) != subplot_len:
        print("Len of X_datas/y_datas is error...")
        return

    if len(titles) != subplot_len:
        titles = ['' for i in range(subplot_len)]

    for i in range(subplot_len):
        ax = fig.add_subplot(rows, cols, i + 1, projection='3d')
        ax.set_title(titles[i])

        X_data = X_datas[i]
        y_data = y_datas[i]

        # pca降维
        X_data = fix_pca(X_data,3)

        # 分类
        X0 = X_data[y_data == 0,]
        X1 = X_data[y_data == 1,]

        # 选取需要显示的点
        X0 = X0[:point_size]
        X1 = X1[:point_size]

        # 绘图
        ax.scatter(X0[:, 0], X0[:, 1], X0[:, 2], c='blue', alpha=0.5)
        ax.scatter(X1[:, 0], X1[:, 1], X1[:, 2], c='red', alpha=0.5, marker='x')

        if i == int((rows-1)*cols+cols/2): # 最后一行的中间
            ax.legend(['benign', 'malware'], loc='center', bbox_to_anchor=(0.5, -0.15), ncol=2)

    return plt

def test_2d():
    # 绘制2d图,子图为1行1列

    # 特征(4个样本,4个特征)
    X_datas = [np.array([[1,1,0,0],[0,1,1,1],[0,0,0,1],[1,0,1,0]])]
    # 前2个为恶意,后2个为良性
    y_datas = [np.array([1,1,0,0])]

    plt = get_plot_2d(1,1,X_datas,y_datas,2)
    plt.show()

def test_3d():
    # 绘制3d图,子图为1行2列

    # 特征1(4个样本,4个特征)
    X_data1 = np.array([[1, 1, 0, 0], [0, 1, 1, 1], [0, 0, 0, 1], [1, 0, 1, 0]])
    # 前2个为恶意,后2个为良性
    y_data1 = np.array([1, 1, 0, 0])

    # 特征2(4个样本,5个特征)
    X_data2 = np.array([[1, 1, 0, 0, 0], [0, 1, 1, 1, 1], [0, 0, 0, 1, 0], [1, 0, 1, 0, 1]])
    # 前2个为恶意,后2个为良性
    y_data2 = np.array([1, 1, 0, 0])

    # 组合成list
    X_datas = [X_data1, X_data2]
    y_datas = [y_data1, y_data2]

    # 调用接口 1行 2列 X_datas y_datas 需要绘制点的数量(每一类为 2个,一共4个)
    plt = get_plot_3d(1, 2, X_datas, y_datas, 2)
    plt.show()

