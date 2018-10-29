



from peepdf.PDFCore import *
#TODO: ADD function


def getpdfhear():
    pass

if __name__ == '__main__':


    # cmd = 'python peepdf/peepdf.py /Users/fengjiaowang/Desktop/yonah/cve2010-2883.pdf'
    # subprocess.call(cmd,shell=True)
    pdfParser = PDFParser()
    #_, pdf = pdfParser.parse('/Users/fengjiaowang/Desktop/yonah/cve2010-2883.pdf')
    file = r"F:\PDFdata\small_data\normalpdf\003.PDF"
    file = r"F:\PDFdata\VirusShare_PDF_20170404\cve2010-2883"
    _, pdf = pdfParser.parse(file)
    #statsDict = pdf.getStats
    tree = pdf.getTree()
    version = pdf.getVersion()
    MD5 = pdf.getMD5()
    streams = pdf.numStreams
    obj = pdf.numObjects
    update = pdf.updates
    comment =len(pdf.comments)
    error = len(pdf.errors)



    print"MD5:",MD5
    print "Version:",version
    print streams,obj
    print update,comment
    print error
    #print "Tree:",tree
    #print tree.get('/Catalog')


    #print pdf.getObject(10).getValue()
    #a = input()
    #pdf.close()
