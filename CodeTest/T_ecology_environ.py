#!/usr/bin/python
# -*- coding: UTF-8 -*-
# python2 


import pymysql
# 连接数据库
def sql_conn():
    """建立数据库连接，返回光标"""
    conn = pymysql.connect(
        host='10.128.2.34',
        user='root',
        password='urunDP2017_',
        charset='utf8'
    )
    return conn  # 根据链接获取可执行SQL语句光标



# 机构与人员配备
def get_people_stuff(year, idxName):
    conn = sql_conn()
    cursor = conn.cursor()
    sql = 'SELECT `value` FROM BaseDB.t_base_ecological_static where hyear = %s and idxName = %s ;'
    res = cursor.execute(sql, [year, idxName])
    if res > 0:
        value = cursor.fetchone()[0]
        if value == '全部符合':
            return '10'
        elif value == '符合三条':
            return '8'
        elif value == '符合两条':
            return '6'
        else:
            return '5'
    conn.close()
    cursor.close()
    return None

# 基础工作制度完善情况
def get_basic_improve(year, idxName):
    conn = sql_conn()
    cursor = conn.cursor()
    sql = 'SELECT `value` FROM BaseDB.t_base_ecological_static where hyear = %s and idxName = %s ;'
    res = cursor.execute(sql, [year, idxName])
    if res > 0:
        value = cursor.fetchone()[0]
        if value == '全部符合':
            return '10'
        elif value == '符合三条':
            return '8'
        elif value == '符合两条':
            return '6'
        else:
            return '5'
    conn.close()
    cursor.close()
    return None

# 业务经费
def get_operational_funds(year, idxName):
    conn = sql_conn()
    cursor = conn.cursor()
    sql = 'SELECT `value` FROM BaseDB.t_base_ecological_static where hyear = %s and idxName = %s ;'
    res = cursor.execute(sql, [year, idxName])
    if res > 0:
        value = cursor.fetchone()[0]
        if value == '全部符合':
            return '10'
        elif value == '符合两条':
            return '10'
        else:
            return '6'
    conn.close()
    cursor.close()   
    return None



def insertDate(year,data):
    conn = sql_conn()
    cursor = conn.cursor()
    # sql = 'update ecology.T_ecology_environ set dept='J04' where dateStr=%s limit 1;'
    # cursor.execute(sql, [year, ])
    sql = 'insert into ecology.T_ecology_environ_year (' \
          'area, dateStr, dept, people_stuff,basic_improve,operational_funds) value (%s, %s, %s, %s, %s, %s)'
    sum = cursor.execute(sql, data)
    conn.commit()
    conn.close()
    cursor.close()
    return sum

def ETC_environ(years):
    for year in years:
        area = '三水区'  # 街道
        dateStr = year  # 时间
        dept = 'J04'  # 来源委办局
        people_stuff = get_people_stuff(year,'机构与人员配备') # 获取机构与人员配备Value
        basic_improve = get_basic_improve(year,'基础工作制度完善情况') # 获取基础工作制度完善工作情况Value
        operational_funds = get_operational_funds(year,'业务经费') # 获取业务经费Value

        # 插入数据
        result_num = insertDate(year, [area, dateStr, dept, people_stuff, basic_improve, operational_funds])
        if result_num > 0:
            print("插入" + str(year) + "年数据成功")
        else:
            print("插入" + str(year) + "年数据失败")

if __name__ == '__main__':
    ETC_environ([2018,2019])