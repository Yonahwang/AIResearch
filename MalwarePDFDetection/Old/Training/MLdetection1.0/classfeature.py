# -*- coding: utf-8 -*-
# !/usr/bin/python


from feature import *
from fileSelection import *

#train_name =['/Users/fengjiaowang/Downloads/data2000/VirusS/cve2010-2883', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a6f8283e82fc8986a5671b06a9f9f0', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00b02fc878456b3d8d3f687462f30513', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a8403a18884ff19bd4c24a744624e6', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a81befe2b7212956dd97ee6b6b0950', '/Users/fengjiaowang/Downloads/data2000/pdf/06dee794d64898b81f0afba0d6d9b478b98f1fc4.pdf', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a91e407c2ce3d67c59128a3c7a9912', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a24fc37a569fea13033857a8d587d1', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_000bb7460c2fd67a6aba77ce3f6b5462', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001eceb1176799bd8b7d678c529e132c', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001e2710555613a82e94156d3ed9c289', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001b454742d182d902e218b92fea47ba', '/Users/fengjiaowang/Downloads/data2000/pdf/0244f92b4889324bc0530c68db1d645947651691.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/03ad6d35f50d7ac18055f182356c71926033ab97.pdf', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001f262e6f215932d9315ee676467646', '/Users/fengjiaowang/Downloads/data2000/pdf/06d644122b1fc7e755c305e4926b620278d5b009.pdf', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a87db13c8a64c6ad1aa457bcd13f75', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a588a64907810dcd5c84941c9f4650', '/Users/fengjiaowang/Downloads/data2000/pdf/02792f8c9fb6277ec946c529cacb136d56967610.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/01e917ca69f6afe1ac13047308710399d9bc730b.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/01aa8162fd7c7910bae1416dfbd8efd0d2723f3d.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/0058a8ee044b833b4d0cee9993cb4175f145075c.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/01d025dad4ef16d50949b1a5aabfc164b84ff5a6.pdf', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a9c6e78f10fe7c9dfd5a3f71443ecc', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001fb4f28610d8aa95aed65bee006be7', '/Users/fengjiaowang/Downloads/data2000/pdf/03d389d998ee2f4e95f54d1a3e86d89e8bf9934f.pdf', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001db6de398995bdd088cd34785c2252', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00000c1d5dcf7543499a66493c3b6bc0', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a51a328feeee951aad537ef25ef2ff', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00b19277c9687b4657f2d2d7aeaaa2ea']
#test_name = ['/Users/fengjiaowang/Downloads/data2000/pdf/01532cd70ee277d6e8363dc90a7e833358fdd0ff.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/02720b158a199004daaa9a655374145fa48f0bd7.PDF', '/Users/fengjiaowang/Downloads/data2000/pdf/035c48b860ef860e5890221f29bbea08d60c0c9d.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/03dd6efeb825cf3910d3b60c38b84153b93d5afb.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/043d0fb1dc75d970d5474d96eef015e81b551d0a.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/0571e300c89c0e0a1c76055b78083473eb066057.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/05f3d3953c6d9e8ed6994ccf9656410510cdcafa.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/05f58384015f06344718da4f3705008700cce16d.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/06b48d974df96e460b585b25d933e7858dec4dd2.pdf', '/Users/fengjiaowang/Downloads/data2000/pdf/07b470c3f97971de9f1d0333f50bafffa107da45.pdf', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_000c62fb36daa4c493437557eb957291', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_000fce225c25ff8b8666f6040fcd8631', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_001b4ee5d4585bde7be24b2425c91d6f', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a0e3c78ac85866d0349d2d8e1f57e0', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a35941566798015552674ccd6fc54f', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a78607d6387e35878fb585bc0fb7cc', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00a93d5da262ab229de88bdb54c35cfb', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00ab49a6766f59687bffc04461cb72b3', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00ac3da69f2af2da0b3f1f960c2d3740', '/Users/fengjiaowang/Downloads/data2000/VirusS/VirusShare_00aeb9bd718e9e66ee66063bcfbc6fdc']
train_feature = []
test_feature = []


def train_featureEX(train):
    #对 train训练数据进行特征提取
    train_dict =dict()

    for f in train:

        #tname = f.split('/')[-1]
        fe_list, fe_key = feature_extract(str(f))
        train_feature.append(fe_list)  # 对输入文件进行特征提取
        #train_dict[str(tname)] = fe_list
    return train_feature

def test_featureEX(test):
    test_dict = dict()
    for f in test:

        #tname = f.split('/')[-1]
        fe_list, fe_key = feature_extract(f)
        test_feature.append(fe_list)  # 对输入文件进行特征提取
        #test_dict[str(tname)] = fe_list
    return test_feature





if __name__ == '__main__':
    start = datetime.datetime.now()
    print train_featureEX(train_name)
    print test_featureEX(test_name)

    end = datetime.datetime.now()
    print "spent time = %d s " %(end - start).seconds