#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess
from PDFCore import PDFParser




def getpdfhear():
    pass

VT_KEY = 'fc90df3f5ac749a94a94cb8bf87e05a681a2eb001aef34b6a0084b8c22c97a64'
if __name__ == '__main__':
    '''
    cmd ='python peepdf.py F:\PDFdata\VirusShare_PDF_20170404\cve2010-2883 -i'
    subprocess.call(cmd,shell=True)
    pdfParser = PDFParser()
    ret, pdf = pdfParser.parse('F:\PDFdata\VirusShare_PDF_20170404\cve2010-2883')
    ps = pdfParser(pdf,VT_KEY,False).do_tree()
    gtr =  pdf.getObject(7)
    print gtr
    #statsDict = pdf.getStats
    '''
    pdfParser = PDFParser()
    ret, pdf = pdfParser.parse('F:\PDFdata\VirusShare_PDF_20170404\cve2010-2883')
    print pdf.getTree()
    print pdf.getObject(12).getValue()
