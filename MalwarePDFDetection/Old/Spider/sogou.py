#!/usr/bin/python
# -*- coding: utf-8 -*-


import urllib.request
from urllib.parse import quote

import re
import requests
import os
import os.path

sogou_domain = "https://www.sogou.com/sogou"

def baidu_search(keyword):
    html=urllib.request.urlopen("https://www.sogou.com/web?query="+quote(keyword)).read()
    return html

def getList(regex,text):
    arr = []
    res = re.findall(regex, text)
    if res:
        for r in res:
            arr.append(r)
    return arr

def getMatch(regex,text):
    res = re.findall(regex, text)
    if res:
        return res[0]
    return ""

def down_file(url,filk_path):
    try:
        res = requests.get(url, stream=True)
    except:
        return
    f = open(filk_path,'wb')
    for chunk in res.iter_content(chunk_size=1024):
        if chunk:
            f.write(chunk)
            f.flush()
    f.close()

# 获取下一页的搜索结果
def get_next_page_url(content):
    page_url = []
    pages = getMatch(r"pagebar_container[\s\S]*?resultbarnum:", content)
    links = getList(r"href=\".*?\">",pages)
    for link in links:
        end = link.find('"',9)
        url = sogou_domain+link[6:end]
        page_url.append(url)
    # page_url[-1] 最后一个url为 下一页的url
    return page_url

# 下载一页的pdf，10个
def down_one_page_of_pdfs(content):
    arrList = getList(r"/images/office/pdf.gif[\s\S]*?class=\"dldoc\">", content)
    for item in arrList:
        regex = r"sogou_vr.*?img"
        link = getMatch(regex, item)
        url = link[27:-6]
        regex = "ext=\"\">.*?</a>"
        title = getMatch(regex, item)[7:-4]

        print('title',title)
        title = title.replace('/','.')
        file_path = 'pdf/' + title + '.pdf'
        if not os.path.isfile(file_path):
            down_file(url, file_path)

html = baidu_search('inurl:".pdf" filetype:pdf')

content = html.decode('utf-8')

#print(content)
page_url = get_next_page_url(content)

down_one_page_of_pdfs(content)

# 下载10页
for i in range(100):
    # page_url[-1] 最后一个url为 下一页的url 一共100页
    html = urllib.request.urlopen(page_url[-1]).read()
    content = html.decode('utf-8')
    page_url = get_next_page_url(content)
    down_one_page_of_pdfs(content)
