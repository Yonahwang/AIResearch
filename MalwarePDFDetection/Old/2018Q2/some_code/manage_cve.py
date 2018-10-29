#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import re
import urllib

uurl = 'https://cve.mitre.org/cgi-bin/cvekey.cgi?keyword=pdf'


def getHtml(url):
    page = urllib.urlopen(url)
    html = page.read()
    return html

def getMatch(regex,text):
    res = re.findall(regex, text)
    if res:
        return res[0]
    return ""


def getList(regex,text):
    arr = []
    res = re.findall(regex, text)
    if res:
        for r in res:
            arr.append(r)
    return arr

def coun(L):
    timm = []
    for i in L:
        tim = i[-13:-9]
        timm.append(tim)
    from collections import Counter
    return Counter(timm)


html = getHtml(uurl)
# Clist = getList(r"cgi-bin/cvekey.cgi.*?",html)
Clist = getList(r"<a href=\"/cgi-bin/cvename[\s\S]*?</a>",html)
dic =  coun(Clist)
print dic
