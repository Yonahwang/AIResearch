# -*- coding: utf-8 -*-
# !/usr/bin/python
from peepdf.PDFCore import *
import datetime





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
        return int(vlue)

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

    try:
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
    except Exception :
        feature['Metadata_len'] = 0
        feature['meta_cration_len'] = 0
        feature['meta_producer_len'] = 0
        feature['meta_creator_len'] = 0
        feature['meta_author_len'] = 0


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

    feature['Versions_num'] = len(statsDict['Versions'])
    Ver_list = []
    for version in range(len(statsDict['Versions'])):
        Ver_dict = {}
        statsVersion = statsDict['Versions'][version]
        obj = statsVersion['Objects'][1]
        obj_size = []
        for ob in obj:
            obj_one = pdf.getObject(ob).getValue()
            obj_size.append(len(obj_one))
        obj_size.sort(cmp=None, key=None, reverse=True)  # 进行降序排序从大到小排序

        if len(obj_size) >= 10:
            Ver_dict['version' + str(version) + 'obj_min'] = min(obj_size)
            Ver_dict['version' + str(version) + '_obj_average'] = sum(obj_size) / len(obj_size)
            for h in range(10):
                Ver_dict['version' + str(version) + '_obj_10_' + str(h)] = obj_size[h]
        elif len(obj_size) > 0:
            Ver_dict['version' + str(version) + '_obj_min'] = min(obj_size)
            Ver_dict['version' + str(version) + '_obj_average'] = sum(obj_size) / len(obj_size)
            while 10 - len(obj_size) > 0:
                obj_size.append(0)
            for h in range(10):
                Ver_dict['version' + str(version) + '_obj_10_' + str(h)] = obj_size[h]
        else:
            Ver_dict['version' + str(version) + '_obj_min'] = 0
            Ver_dict['version' + str(version) + '_obj_average'] = 0
            for h in range(10):
                Ver_dict['version' + str(version) + '_obj_10_' + str(h)] = 0

        stream = statsVersion['Streams'][1]
        stream_size = []
        for s in stream:
            stream_one = pdf.getObject(s).getValue()
            stream_size.append(len(stream_one))
        stream_size.sort()  # 从小到大排序
        if len(stream_size) >= 2:
            for h in range(2):
                Ver_dict['version' + str(version) + '_stream_min_' + str(h)] = stream_size[h]
        else:
            while 2 - len(stream_size) > 0:
                stream_size.append(0)
            stream_size.sort()
            for h in range(2):
                Ver_dict['version' + str(version) + '_stream_min_' + str(h)] = stream_size[h]

        Actions_JS = 0
        Actions_javascript = 0
        if statsVersion['Actions'] != None:
            for i in statsVersion['Actions']:
                if i == '/JS':
                    Actions_JS = 1
                if i == '/JavaScript':
                    Actions_javascript = 1
        Ver_dict['version' + str(version) + '_Actions_javascript'] = Actions_javascript
        Ver_dict['version' + str(version) + '_Actions_JS'] = Actions_JS

        Events_AA = 0
        Events_Names = 0
        if statsVersion['Events'] != None:
            for i in statsVersion['Events']:
                if i == '/AA':
                    Events_AA = 1
                if i == '/Names':
                    Events_Names = 1
        Ver_dict['version' + str(version) + '_Events_AA'] = Events_AA
        Ver_dict['version' + str(version) + '_Events_Names'] = Events_Names

        EmbeddedFiles = 0
        if statsVersion['Elements'] != None:
            for i in statsVersion['Elements']:
                if i == '/EmbeddedFiles':
                    EmbeddedFiles = 1
        Ver_dict['version' + str(version) + '_Elements_EmbeddedFiles'] = EmbeddedFiles

        Ver_dict['version' + str(version) + '_Catalog'] = None_vlue(statsVersion['Catalog'])
        Ver_dict['version' + str(version) + '_Xref Streams'] = None_int(statsVersion['Xref Streams'])
        Ver_dict['version' + str(version) + '_elements'] = None_len(statsVersion['Elements'])
        Ver_dict['version' + str(version) + '_Events_num'] = None_len(statsVersion['Events'])
        Ver_dict['version' + str(version) + '_Actions_num'] = None_len(statsVersion['Actions'])
        Ver_dict['version' + str(version) + '_Vulns'] = None_len(statsVersion['Vulns'])
        Ver_dict['version' + str(version) + '_Encoded_num'] = None_int(statsVersion['Encoded'])
        Ver_dict['version' + str(version) + '_Objects_JS_num'] = None_int(statsVersion['Objects with JS code'])
        Ver_dict['version' + str(version) + '_Compressd_obj'] = None_int(statsVersion['Compressed Objects'])
        Ver_dict['version' + str(version) + '_Info'] = None_int(statsVersion['Info'])
        Ver_dict['version' + str(version) + '_Object Streams'] = None_int(statsVersion['Object Streams'])
        Ver_dict['version' + str(version) + '_Streams'] = None_int(statsVersion['Streams'])
        try:
            Decoding_Errors = None_int(statsVersion['Decoding Errors'])
        except:
            Decoding_Errors = 0
        Ver_dict['version' + str(version) + '_Decoding Errors'] = Decoding_Errors  # error
        Ver_list.append(Ver_dict)

    if len(Ver_list) >= 3:
        for v in range(3):
            for i in Ver_list[v]:
                feature[i] = Ver_list[v][i]
    if len(Ver_list) < 3:
        Ver_dict2 = {}
        for vj in range(len(Ver_list), 3):
            for d in Ver_list[0]:
                Ver_dict2[str(vj) + d] = 0
            Ver_list.append(Ver_dict2)
        for v in range(len(Ver_list)):
            for i in Ver_list[v]:
                feature[i] = Ver_list[v][i]



    feature['Binary'] = bool_change(statsDict['Binary'])
    feature['Linearized'] = bool_change(statsDict['Linearized'])
    feature['Encrypted'] = bool_change(statsDict['Encrypted'])
    feature['Metadata'] = None_len(pdf.getMetadata())
    version = pdf.version
    version = version.split('.',1)
    feature['version-1'] = int(version[0])
    feature['version-2'] = int(version[1])
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

