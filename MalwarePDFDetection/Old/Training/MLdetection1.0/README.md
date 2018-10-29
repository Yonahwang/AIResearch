# Machine Learning {PDF} Malware Detection Engine

## Time Range
 - 2017 Q4

## Project Status
 - On Schedule (2017/11/20)
 - On Schedule (2017/09/30)
 - ~~On track again (2017/09/20)~~
 - ~~Paused due to role **reassignment** (2017/09/11)~~
 - ~~On Schedule~~ 

## Research Scientist
 - [Fengjiao Wang]()
 - [Wei Jiang]()

## OKR
### Objective
- Construction of Machine Learning {PDF} Malware Detection and Classification Engine Research Prototype

### Key Results（可量化的关键成果）
在当前PDF检测引擎基础上，从 速度 & 质量 2方面 改善模型：

9. --> 使用Python使用大数据计算平台Spark生成模型（而不是现在的单机，单模型生成） -> In Progress
10. 撰写论文样式的小论文，分析报告等形式
11. 学习大数据分析技术，处理数据量级达到百万，input 是全体数据（还有可能全部是病毒，需要自己寻找正常样本去构造模型）
（我们已经有百万级，我们已经有大数据平台，操作这些数据吧，弄个分布式模型的原型）




1. 编写 单机 检测程序 1个 **DONE**
2. 收集数据样本集 1份 **DONE**
3. 模型分类准确度：在 中型数据集 下，模型识别能力达 >= 50% (注：中型数据集 = 目前实验所用 + Hadoop集群上的数据集) **DONE**
4. 输出实验报告 1份 **DONE**
5. 数据量达 万级 **DONE**
6. 数据量达 十万级 **DONE**
7. 模型识别能力达 >= 90% **DONE**  
8. --> 分析**每一个**模型预测错误的PDFsample，并归纳原因 **~Hundreds**


#### Roadmap
（Updated on 2017/12/5）
- 1）	数据集：
Normal sample number  :  897
Malware sample number  :  83442 
在做训练和推测的时候，随机抽取其中的部分做训练和测试做dataset 
- 2） 特征提取：
对提取到的特征做特征优化，目前按照参考论文，目前提取的特征有133个 ，以下是一些重要特征的分布图
![image](https://github.com/Yonahwang/God_with_me/blob/master/pdf-git/featureim.png)
![image](https://github.com/Yonahwang/God_with_me/blob/master/pdf-git/feature2.png)

- 3）模型识别率：
- 以下是在样本中随机抽取2300个样本做出的一个预测结果截图：
![image](https://github.com/Yonahwang/God_with_me/blob/master/pdf-git/solution2300.png)
- 如图是是在样本中随机抽取2355个样本做出的一个预测结果截图：
![image](https://github.com/Yonahwang/God_with_me/blob/master/pdf-git/solution2355.png)

（Updated on 2017/11/13）
- 1. 充分利用现有数据，对提取到的特征做特征优化，目前按照参考论文，提取的又有特征有54个
- 2.增加对PDF文件解析的数量，主要主对FN/FT 文件进行优化和解析
- 3.参考国内外对PDF文件的解析，做实验原型的验证，目标提取有用特征200个,模型识别能力达 >= 98.00%

（Updated on 2017/10/30）
- 1. 充分利用现有数据，对提取到的特征做特征优化，优化后分析样本1000个，模型识别能力达 >= 97.00%
- 2. 增加对PDF文件解析的数量，由之前的各位提升到十位，争取破百位
- 3. 参考国内外对PDF文件的解析，做实验原型的验证，目标提取有用特征200个,更新之前的特征提取，现可用特征提取有37个

（Updated on 2017/10/11）
- 1.针对爬取到的数据，进行清理，分类出可用的做数据测试
- 2.增加可用测试样本到1000 ，提取样本特征143 个，模型识别能力达 >= 92.98%
- 3.针对特征提取和分析算法上的改进，参考国内外论文，做一步一步的实践

(Updated on 2017/09/29)
- 1.平衡PDF数据集样本，爬取测试样本到万 级，恶意样本 到十万 级
- 2.应用到机器学习框架来判断分析PDF文件，提取特征 143 个，分析样本500 个，模型识别能力达 >=91.89%
- 3.ing 深入分析PDF文件，不断恶意代码识别的优化及算法，参考国内外论文进行下一步深入分析

(Updated on 2017/08/28)
- 1.收集恶意PDF文件 156035 个（目前已经收集到 十万级）
- 2.基于ML技术框架，添加PDF特征提取和解析，完成一个完整的检测过程，输出检测结果
- 3.输出实验报告和结果分析，并不断优化迭代

Note: 目前对PDF的特征提取,主要是利用python内置包（如pdfminer）和导入开源包peepdf等，参考一些前沿技术文章对一下特征进行提取：

| Feature | Detail | Remarks |
| ------| ------ | ------ |
|the author metadata item 作者元数据项目|== producer_len ==| 
|the author metadata item 作者元数据项目| producer_oth | 
|the author metadata item 作者元数据项目| producer_uc | 
|objects/streams| ==len(objects)== | 
|objects/streams| ==数量(count)==  |
|objects/streams| location | 
|objects/streams| count_stream_diff | 
|objects/streams| len_stream_min | 
|images | len() | 
|images| location | 
|images| image_totalpx | 
|images| ratio_imagepx_size | 
|数据编码方法|==data encoding methods==|
|object types| ==计数加密对象（count of encryption objects）==|
|计数JavaScript|==len(objects)==|
| 计数JavaScript |==数量(count)==
|计数JavaScript|location
|计数js|==len(objects)==
|计数js|==数量(count)==
|计数js|location
|Font|==count_font==
|time|createdate_tz|
|time|moddate_tz
|object|==count_obj==
|object|count_endobj
||pdfid0_mismatch|
||pos_eof_avg|
||pos_eof_max|
|Boxes|pos_box_max|
||creator_len
||ref_min_id
||title_len
||title_lc
||count_filter_obs
||pos_ref_avg
|version|==version==
||==len_URLs==
||==openAction==



下一步:
增加测试数量集，优化特征提取，增加优化算法
- 1.用爬虫不断增加测试样本数量级，
- 2.深入分析PDF文件，不断恶意代码识别的优化及算法，参考国内外论文进行下一步深入分析

## Peer Code Review
(Updated on 2017/09/07 by Deyuan Li)
- 1.加深对PDF文件格式以及PDF病毒常见的文件特征的认识，可结合具体的文献资料，起步时可采用小数据集（理想化的数据）&简单特征
- 2.特征需要量化才能作为机器学习模型的输入，并且可通过统计分析的方法证明特征的有效性

## How to Compile / Run the code 
- ####Dependencies
-PyV8 (and V8) (optional: if you intend to use JS deobfuscation. Note: JS deobfuscation needs to be run in a safe environment, as you would treat any malware.
-lxml
-scandir (optional: module included in lib folder)
-postgresql and psycopg2 (optional: if you intend to use postgresql backing storage)
####Open Source PDF Tools

-peePDF
-PDFMiner
-swf mastah

####How does it work?
- How do you perform this tool to analyze if your PDF is malicious?
~The basic syntax is:

~~$ python peepdf.py pdf_file
   
 ~But you can use the -f option to avoid errors and to force the tool to ignore them:

~~$ python peepdf.py fcexploit.pdf

- Now you can get the following information

- MD5
- SHA1
- Catalog
- Objects with JS code
- Suspicious elements
- Size
- ...

## Python Project Structure
Q: Imagine that you want to develop a non-trivial end-user desktop (not web) application in Python.What is the best way to structure the project's folder hierarchy? Desirable features are ease of maintenance, IDE-friendliness, suitability for source control branching/merging, and easy generation of install packages. In particular:
  - Where do you put the source?
  - Where do you put application startup scripts?
  - Where do you put the IDE project cruft?
  - Where do you put the unit/acceptance tests?
  - Where do you put non-Python data such as config files?
  - Where do you put non-Python sources such as C++ for pyd/so binary extension modules?

A: Doesn't too much matter. Whatever makes you happy will work. There aren't a lot of silly rules because Python projects can be simple.
  - /scripts or /bin for that kind of command-line interface stuff
  - /tests for your tests
  - /lib for your C-language libraries
  - /doc for most documentation
  - /apidoc for the Epydoc-generated API docs

## Q&A
 - (This section should be answered by **Fengjiao**)

### Accuracy
  - Upon how much data does the machine learning solution base its decisions? Is it enough?

  - From where does the data come? Is there a wide variety of sources, or are they dependent on third-party threat aggregator sites?

  - How often is the data collected?

  - How often are new models trained and propagated to the customer?

  - How is the system trained? Is it trained through a constant supply of rich data sets, so properties discovered can be used in future machine learning decisions?

  - How does the vendor handle false positives?

  - How does the vendor handle false negatives that the vendor later discovers (after the customer has run the malware)?

### Speed
  - How quickly can the solution make a determination that leads to action?

  - How quickly can it obtain enough relevant new data to influence the decisions it makes?

### Efficiency
  - Where and how quickly does the analysis take place?

  - What is the impact on the end-user system?

  - What type of analysis is done on incoming files? On endpoints only, on cloud only, or a combination?

  - Does it rely on post-event analysis (detecting rather than preventing)?

## References
- [PlatPal: Detecting Malicious Documents with Platform Diversity USENIXSec 2017](https://www.dropbox.com/sh/5nwwv0algh2jpg6/AABadLuyfTWyB_-Lz6ZzAXdVa?dl=0)
