# -*- coding: utf-8 -*-
# !/usr/bin/python
from peepdf.PDFCore import *

def fakeFile_check(filePath):
    try:
        from peepdf.PDFCore import PDFParser
        pdfParser = PDFParser()
        _, pdf = pdfParser.parse(filePath)
        return pdf
    except Exception:
        return None

# bool change to string
def bool_change(vlue):
    if vlue == True:
        return 0
    else:
        return 1


def None_int(vlue):
    if vlue == None:
        return 0
    else:
        return int(vlue[0])

def None_len(vlue):
    if vlue == None:
        return 0
    else:
        return len(vlue)

def None_vlue(vlue):
    if vlue == None:
        return 0
    else:
        return vlue

def YESorNO(vlue):
    if len(vlue) != 0:
        return len(vlue)
    else:
        return 0


def feature_extract(pdf):  # 对输入文件进行特征提取

    feature = dict()
    f_id = []
    statsDict = pdf.getStats()


    trailer = pdf.getTrailer()
    if trailer[0] == 0:
        feature['trailer_num'] = 0
    elif trailer[0] != 0:
        feature['trailer_num'] = trailer[0]


    XrefSection = pdf.getXrefSection()
    feature['XrefSection'] = len(XrefSection)
    Xref = XrefSection[1][0]
    if Xref != None:
        feature['Xref_size'] = Xref.size
        feature['Xref_stream'] = None_int(Xref.streamObject)
        feature['Xref_offset'] = Xref.offset
        feature['Xref_bytesPerFisId'] = len(Xref.bytesPerField)
        feature['Xref_errors'] = len(Xref.errors)
        subsections = XrefSection[1][0].subsections[0]
        if subsections != None:
            feature['subsections_size'] = subsections.size
            feature['subsections_numObjects'] = subsections.numObjects
            feature['subsections_firstObject'] = subsections.firstObject
            feature['subsections_offset'] = subsections.offset
            feature['subsections_entries'] = len(subsections.entries)
            feature['subsections_errors'] = len(subsections.errors)
        if subsections == None:
            feature['subsections_size'] = 0
            feature['subsections_numObjects'] = 0
            feature['subsections_firstObject'] = 0
            feature['subsections_offset'] = 0
            feature['subsections_entries'] = 0
            feature['subsections_errors'] = 0

    elif Xref == None:
        feature['Xref_size'] = 0
        feature['Xref_stream'] = 0
        feature['Xref_offset'] = 0
        feature['Xref_bytesPerFisId'] = 0
        feature['Xref_errors'] = 0
        if XrefSection[1][1] != None:
            subsections = XrefSection[1][1].subsections[0]
            if subsections != None:
                feature['subsections_size'] = subsections.size
                feature['subsections_numObjects'] = subsections.numObjects
                feature['subsections_firstObject'] = subsections.firstObject
                feature['subsections_offset'] = subsections.offset
                feature['subsections_entries'] = len(subsections.entries)
                feature['subsections_errors'] = len(subsections.errors)
            if subsections == None:
                feature['subsections_size'] = 0
                feature['subsections_numObjects'] = 0
                feature['subsections_firstObject'] = 0
                feature['subsections_offset'] = 0
                feature['subsections_entries'] = 0
                feature['subsections_errors'] = 0
        if XrefSection[1][1] == None:
            feature['subsections_size'] = 0
            feature['subsections_numObjects'] = 0
            feature['subsections_firstObject'] = 0
            feature['subsections_offset'] = 0
            feature['subsections_entries'] = 0
            feature['subsections_errors'] = 0


    Metadata = pdf.getBasicMetadata(0)
    feature['Metadata_len'] = None_len(Metadata)
    meta_creation = ''
    meta_producer = ''
    meta_creator = ''
    meta_author = ''
    for k in Metadata:
        if k == 'creation':
            meta_creation = Metadata[k]
        elif k == 'producer':
            meta_producer = Metadata[k]
        elif k == 'creator':
            meta_creator = Metadata[k]
        elif k == 'author':
            meta_author = Metadata[k]
    feature['meta_cration_len'] = YESorNO(meta_creation)
    feature['meta_producer_len'] = YESorNO(meta_producer)
    feature['meta_creator_len'] = YESorNO(meta_creator)
    feature['meta_author_len'] = YESorNO(meta_author)
    '''if len(meta_creation) != 0:
        timeC = meta_creation[2:16]
        feature['meta_creation'] = meta_creation[2:16]
    else:
        feature['meta_creation'] = 0'''

    gtree = pdf.getTree()
    font_count = 0
    Javascript_count = 0
    JS_count = 0
    acd = gtree[len(gtree) - 1][1]
    for k in acd:
        if '/Font' == acd[k][0]:
            font_count += 1
        if '/JavaScript' == acd[k][0]:
            Javascript_count += 1
        if '/JS' == acd[k][0]:
            JS_count += 1
    feature['font_count'] = font_count
    feature['Javascript_count'] = Javascript_count
    feature['JS_count'] = JS_count

    feature['JS_MODULE'] = bool_change(JS_MODULE)


    for version in range(len(statsDict['Versions'])):

        statsVersion = statsDict['Versions'][version]
        obj = statsVersion['Objects'][1]
        obj_size = []
        for ob in obj:
            obj_one = pdf.getObject(ob).getValue()
            obj_size.append(len(obj_one))
        obj_size.sort(cmp=None, key=None, reverse=True)  # 进行降序排序从大到小排序
        if len(obj_size) >= 10:
            for h in range(10):
                feature['obj_10_' + str(h)] = obj_size[h]
        else:
            while 10 - len(obj_size) > 0:
                obj_size.append(0)
            for h in range(10):
                feature['obj_10_' + str(h)] = obj_size[h]

        stream = statsVersion['Streams'][1]
        stream_size = []
        for s in stream:
            stream_one = pdf.getObject(s).getValue()
            stream_size.append(len(stream_one))
        stream_size.sort()  # 从小到大排序
        if len(stream_size) >= 2:
            for h in range(2):
                feature['stream_min_' + str(h)] = stream_size[h]
        else:
            while 2 - len(stream_size) > 0:
                stream_size.append(0)
                stream_size.sort()
            for h in range(2):
                feature['stream_min_' + str(h)] = stream_size[h]

        Actions_JS = 0
        Actions_javascript = 0
        if statsVersion['Actions'] != None:
            for i in statsVersion['Actions']:
                if i == '/JS':
                    Actions_JS = 1
                if i == '/JavaScript':
                    Actions_javascript = 1
        feature['Actions_javascript'] = Actions_javascript
        feature['Actions_JS'] = Actions_JS


        feature['Catalog'] = None_vlue(statsVersion['Catalog'])
        feature['Xref Streams'] = None_int(statsVersion['Xref Streams'])
        feature['elements'] = None_len(statsVersion['Elements'])
        feature['Events_num'] = None_len(statsVersion['Events'])
        feature['Actions_num'] = None_len(statsVersion['Actions'])
        feature['Vulns'] = None_len(statsVersion['Vulns'])
        feature['Encoded_num'] = None_int(statsVersion['Encoded'])
        feature['Objects_JS_num'] = None_int(statsVersion['Objects with JS code'])
        feature['Compressd_obj'] = None_int(statsVersion['Compressed Objects'])
        feature['Xref Streams'] = None_int(statsVersion['Xref Streams'])
        feature['Info'] = None_int(statsVersion['Info'])
        feature['Object Streams'] = None_int(statsVersion['Object Streams'])
        feature['Streams'] = None_int(statsVersion['Streams'])
        #feature['Decoding Errors'] = None_int(statsVersion['Decoding Errors'])

    '''sha1 = pdf.getSHA1()
    for h in range(len(sha1)):
        feature['sha1_' + str(h)] = int(sha1[h], 16)
    md5 = pdf.getMD5()
    for g in range(len(md5)):
        feature['md5_' + str(g)] = int(md5[g], 16)'''

    feature['Binary'] = bool_change(statsDict['Binary'])
    feature['Linearized'] = bool_change(statsDict['Linearized'])
    feature['Encrypted'] = bool_change(statsDict['Encrypted'])
    feature['Metadata'] = None_len(pdf.getMetadata())
    feature['version'] = pdf.version
    feature['stream_num'] = pdf.numStreams
    feature['file_size'] = pdf.getSize()
    feature['object_num'] = pdf.numObjects
    feature['update'] = pdf.getNumUpdates()
    feature['comments'] = len(pdf.comments)
    feature['error'] = len(pdf.errors)
    feature['len_URLs'] = len(pdf.getURLs())
    feature['numEncodedStreams'] = pdf.numEncodedStreams
    #feature['Catalog_id'] = pdf.getCatalogObjectId()
    feature['header_offset'] = pdf.getHeaderOffset()

    for i in feature:
        f_id.append(i)




    return [feature[k] for k in feature],f_id