package jobOneHour.subJob.detectDGA

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobOneHour.subJob.detectDGA.classmethod.commonFunc
import jobOneHour.subJob.detectDGA.classmethod.trainFeature
import org.apache.log4j.PropertyConfigurator
import jobOneHour.subClass.saveToKAFKA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.io.BufferedSource

/**
  * Created by Yiutto on 2018年7月2日 0015.
  */
/**
  *
  * @param dataDF
  * @param happentimeH (示例："2018061412")只能处理当前时刻的一小时的数据(12:00-13:00)(格式为yyyyMMddHH)
  * @param mysparksession
  * @param properties
  */
class dgaDetectMain(dataDF: DataFrame, happentimeH: String, mysparksession: SparkSession, properties: Properties)
  extends commonFunc with saveToKAFKA with Serializable {

  def main(): Unit = {

    //A.读取配置文件

    //（用于服务器Linux）
    val filePath = new File("..").getAbsolutePath
    //    val filePath = System.getProperty("user.dir")
    //log日志文件路径
    PropertyConfigurator.configure(filePath + "/conf/detectDGA_log4j.properties")

    // 读取本地文件，作为相关函数的参数
    val (corr_args, model) = read_corr_configuration()

    //c.6 保存dga域名路径
    val dgaPath = properties.getProperty("save.dga.path")
    //c.7 设置域名的长度阈值
    val domainLen = properties.getProperty("dns.domain.length").toInt
    //c.8 设置1h内访问dga域名的最小次数
    val countMin = properties.getProperty("skim.dga.nums").toInt
    //c.9 设置1h内访问dga域名的最小个数
    val dgasMin = properties.getProperty("dns.dga.nums").toInt
    //c.10 读取kafka相关结点及topic
    val kafka_node = properties.getProperty("kafka.nodes")
    val kafka_topic = properties.getProperty("kafka.topic2")

    println("----")
    // 原则上跑一次循环最大时间不能超过1小时)
    try {
      // 获取HDFS上的DNS数据整合溯源信息
      // 字段为"ip", "queries_name", "source", "position"
      val dataDFPerHour: DataFrame = traceDnsSource(dataDF)
      //      dataDFPerHour.show(5, false)
      // 加载内存中
      dataDFPerHour.persist()

      val trainFeature = new trainFeature()
      // 剔除DNS中一些无效数据，并根据里面的query_name字段建立特征工程
      val dnsDataFeature: RDD[(String, String, Array[Double])] = featureProcessDNS(trainFeature, dataDFPerHour, corr_args, domainLen)

      // 获取DGA相关结果表
      val dgaDF = getDGAData(dnsDataFeature, model)
      val dgaNums = dgaDF.count()

      if (dgaNums > 0) {
        // 得到最终的事件溯源信息表（"ip", "dga", "evidence", "source", "count"，"position")
        val eventSourceDF = getEventSource(dataDFPerHour, dgaDF, countMin, dgasMin)

        logger.error("准备保存" + happentimeH + "一小时内DNS数据产生的DGA域名>>>>>" + dgaPath)

        if (eventSourceDF != null) {
          // 保存DGA域名到本地（精确度高一点）
          writeFile(eventSourceDF, dgaPath)

          //-----------------------------入Kafka和保存文件操作----------------------------------
          // 构造kafka需要的相关数据
          val (record_nums, kafkaDnsDF) = makeKafkaDnsData(eventSourceDF, happentimeH)
          //          kafkaDnsDF.show()
          // 把数据写入kafka
          toKafka(mysparksession, (kafka_node, kafka_topic), kafkaDnsDF)

          logger.error(happentimeH + "当前时刻的一小时内访问可疑域名的ip记录共有" + record_nums + "条，已完成入Kafka")
        } else {
          logger.error("未检测到网元ip多次访问dga域名或者访问的dga域名个数过少，不予入Kafka")
        }

      } else {
        logger.error(happentimeH + "前一小时内DNS数据未检测到DGA域名")
      }

      // 释放资源
      dataDFPerHour.unpersist()
    }

    catch {
      case e: Exception => logger.error("Error：" + e.getMessage)
    } finally {
    }

  }


  def read_corr_configuration(): ((Array[String], Map[String, Int],
    mutable.HashMap[String, mutable.HashMap[String, Double]], Array[String]), GradientBoostedTreesModel) = {

    val sc = mysparksession.sparkContext
    logger.error("Load configuration file, waiting........")

    //C.获取相关文件路径
    val hdfsUrl = properties.getProperty("hdfs.path")
    //c.1 顶级域名获取列表路径
    val suffixPath = hdfsUrl + properties.getProperty("suffix.Path.name")
    //c.2 n-gram在Alexa的排名
    val saveNgramRankPath = hdfsUrl + properties.getProperty("NgramRank.Path.name")
    //c.3 hmm转移概率路径
    val saveHmmProPath = hdfsUrl + properties.getProperty("HmmPro.Path.name")
    //c.4 读取domain的白名单
    val white_domainPath = hdfsUrl + properties.getProperty("white.domain.path")
    //c.5 获取模型训练路径
    val modelPath = hdfsUrl + properties.getProperty("model.Path.name")

    //D.数据预处理
    //d.1 读取suffix，public_suffix_list.dat,已知的tld(top level domain)
    val suffixArr: Array[String] = readText(mysparksession, suffixPath)
    val idx_ICANN_DOMAIN: Int = suffixArr.indexOf("// ===BEGIN PRIVATE DOMAINS===")
    val tldsArray: Array[String] = suffixArr.slice(0, idx_ICANN_DOMAIN).filter(line =>
      ("//".r).findAllIn(line).isEmpty && (line != ""))

    //d.2 读取Top100Alexa的n-gram模型的排名,将topAlexaNgramRank.csv中的word、rank转换为字典
    val AlexaNgramRank: DataFrame = readCsv(mysparksession, saveNgramRankPath)
    val ngramRank: Map[String, Int] = AlexaNgramRank.rdd.map { line =>
      val word = line.getAs[String]("word")
      val rank = line.getAs[String]("rank").toInt
      (word, rank)
    }.collect().toMap

    //d.3 读取Top100Alexa的hmm模型中bigram之间的转移概率
    val BigraHmmPro: DataFrame = readCsv(mysparksession, saveHmmProPath)
    val hmmPro: mutable.HashMap[String, mutable.HashMap[String, Double]] = hmmProMap(BigraHmmPro)

    //d.4 读取白名单domain
    val white_domainArr = sc.textFile(white_domainPath).collect()
    logger.error("Sucess read:" + white_domainPath)

    //d.5 加载本地保存的GradientBoostedTree模型
    logger.error("Sucess read:" + modelPath)
    val model: GradientBoostedTreesModel = GradientBoostedTreesModel.load(sc, modelPath)
    logger.error("Load configuration file, finished!")

    ((tldsArray, ngramRank, hmmPro, white_domainArr), model)
  }


  /**
    * 将hmm的转移概率转换为HashMap，方便提取
    *
    * @param dataframe
    * @return
    */
  def hmmProMap(dataframe: DataFrame): mutable.HashMap[String, mutable.HashMap[String, Double]] = {
    val HmmPro: RDD[(String, Iterable[(String, Double)])] = dataframe.rdd.map { line =>
      (line.getAs[String]("bigramPre"), (line.getAs[String]("bigramNext"), line.getAs[String]("pro").toDouble))
    }.groupByKey()
    val transGram = new mutable.HashMap[String, mutable.HashMap[String, Double]]()
    for (line1 <- HmmPro.collect()) {
      val s1 = line1._1
      val s2 = line1._2
      if (s1 == null) {
        if (!transGram.contains(""))
          transGram("") = mutable.HashMap[String, Double]()
        for (line2 <- s2) {
          transGram("") += (line2._1 -> line2._2)
        }
      }
      else {
        if (!transGram.contains(s1))
          transGram(s1) = mutable.HashMap[String, Double]()
        for (line2 <- s2) {
          transGram(s1) += (line2._1 -> line2._2)
        }
      }
    }
    transGram
  }

  /**
    * 根据dns数据中的"queries_name"进行简单过滤等数据预处理操作，并建立特征向量
    *
    * @param trainFeature
    * @param dnsData
    * @param corr_arg
    * @param len
    * @return
    */
  def featureProcessDNS(trainFeature: trainFeature, dnsData: DataFrame,
                        corr_arg: (Array[String], Map[String, Int],
                          mutable.HashMap[String, mutable.HashMap[String, Double]], Array[String]),
                        len: Int): RDD[(String, String, Array[Double])] = {

    import mysparksession.implicits._
    val (tldsArray, ngramRank, hmmPro, white_domainArr) = corr_arg

    // 设置过滤的suffix(说白了其实就是顶级域名)
    val filter_suffix = List("in-addr.arpa", "ip6.arpa", "No-Parse", "gov.cn")
    // 读取dns_log数据中(domain.length>=7
    val dnsHostDF = dnsData.select("queries_name").distinct()
      .map {
        line =>
          val host: String = line.getAs[String]("queries_name")
          val (subdomain, domain, suffix) = trainFeature.tldextract(host, tldsArray)
          (host, subdomain, domain, suffix)
      }.toDF("host", "subdomain", "domain", "suffix")
      .filter {
        line =>
          val sublen = line.getAs[String]("subdomain").split("\\.").length
          // 新加条件
          val domain = line.getAs[String]("domain")
          val suffix = line.getAs[String]("suffix")
          (sublen <= 2) && (domain.length >= len) && !(filter_suffix.contains(suffix)) && !(white_domainArr.contains(domain))
      }

    // -------------对dnsHostDF数据建立特征工程------------------
    val dnsDataFeature = dnsHostDF.rdd.map {
      line =>
        val host: String = line.getAs[String]("host")
        val (domain, features) = trainFeature.getFeature(host, tldsArray, ngramRank, hmmPro)
        (host, domain, features)
    }

    dnsDataFeature
  }


  /**
    * 根据之前建立的GBTmodel模型，判断域名是否为dga
    *
    * @param dnsDataFeature
    * @param GBTmodel
    * @return
    */
  def getDGAData(dnsDataFeature: RDD[(String, String, Array[Double])],
                 GBTmodel: GradientBoostedTreesModel): DataFrame = {
    import mysparksession.implicits._
    val dgaDataDF = dnsDataFeature.map { line =>
      val host = line._1
      val domain = line._2
      //      feature = Array(len, entropy, unigramAvgRank, bigramAvgRank, trigramAvgRank, vowelpro,
      //        digitpro, reletterpro, conti_consonantpro, conti_digitpro, -getdomainpro)
      val feature: Array[String] = line._3.map(_.formatted("%.2f").toString)
      val evidence: String = "DGA域名：" + host +
        "，其中信息熵：" + feature(1) +
        "，元音字母概率：" + feature(5) +
        "，重复字母概率：" + feature(7) +
        "，HMM系数：" + feature(10)
      (host, evidence, GBTmodel.predict(Vectors.dense(line._3)))
    }.toDF("queries_name", "evidence", "label").where("label=1")
    dgaDataDF.select("queries_name", "evidence").toDF()
  }


  /**
    * 将dns数据进行整合，增加溯源字段source, position
    *
    * @param encryptedPort
    * @return
    */
  def traceDnsSource(encryptedPort: DataFrame): DataFrame = {
    // 将五元组组合成一个字段
    val eventSourceUdf = org.apache.spark.sql.functions.udf(
      (srcip: String, dstip: String, srcport: String, dstport: String, protocol: String) => {
        srcip + "," + dstip + "," + srcport + "," + dstport + "," + protocol
      }
    )

    // 将源目的ip的地理位置整合成一个字段
    val positionUdf = org.apache.spark.sql.functions.udf(
      (scountry: String, sprovince: String, scity: String, slatitude: String, slongitude: String,
       dcountry: String, dprovince: String, dcity: String, dlatitude: String, dlongitude: String) => {
        scountry + "," + sprovince + "," + scity + "," + slatitude + "," + slongitude + "," +
          dcountry + "," + dprovince + "," + dcity + "," + dlatitude + "," + dlongitude
      }
    )

    val DF = encryptedPort.withColumn("source", eventSourceUdf(encryptedPort("srcip"), encryptedPort("dstip"),
      encryptedPort("srcport"), encryptedPort("dstport"), encryptedPort("protocol")))
      .withColumn("position", positionUdf(encryptedPort("scountry"), encryptedPort("sprovince"),
        encryptedPort("scity"), encryptedPort("slatitude"), encryptedPort("slongitude"),
        encryptedPort("dcountry"), encryptedPort("dprovince"), encryptedPort("dcity"),
        encryptedPort("dlatitude"), encryptedPort("dlongitude")))


    DF.select("srcip", "queries_name", "source", "position").toDF("ip", "queries_name", "source", "position")
  }

  /**
    * 通过"queries_name"来关联dga域名和dns数据，得到相应的事件溯源表
    * 最终结果的col有3个"ip","dgas","sources"，"counts"
    *
    * @param sourceDF
    * @param dgaDF
    * @return
    */
  def getEventSource(sourceDF: DataFrame, dgaDF: DataFrame, countMin: Int, dgasMin: Int): DataFrame = {
    //    sourceDF("ip", "queries_name", "source", "position"
    //    dgaDF("queries_name", "evidence")

    import mysparksession.implicits._
    // 将dga域名关联源dns日志
    val joinedDF = sourceDF.join(dgaDF, "queries_name").select("ip", "queries_name", "evidence", "source", "position")

    val ip_dgaUdf = org.apache.spark.sql.functions.udf((ip: String, queries_name: String, evidence: String) => {
      ip + "#" + queries_name + "#" + evidence
    })

    // 将ip和dga、evidence绑定一起, 方便统计该ip访问dga的次数
    val ipDgaEvidenceDF = joinedDF.withColumn("ip_dga_evidence", ip_dgaUdf(joinedDF("ip"), joinedDF("queries_name"),
      joinedDF("evidence")))

    // 对ip_dga_evidence进行聚合，将所有source合并一起
    val ipDgaSourceDF = ipDgaEvidenceDF.select("ip_dga_evidence", "source", "position").groupBy("ip_dga_evidence")
      .agg(concat_ws("#", collect_set("source")), concat_ws("#", collect_set("position")))
      .withColumnRenamed("concat_ws(#, collect_set(source))", "sources")
      .withColumnRenamed("concat_ws(#, collect_set(position))", "positions")

    // 将ip_dga_evidence进行拆分，统计当前ip访问dga的次数，提取相关溯源信息
    val tempRDD1: RDD[(String, String, String, String, String, String)] = ipDgaSourceDF.rdd.map { line =>
      val ip = line.getAs[String]("ip_dga_evidence").split("#")(0)
      val dga = line.getAs[String]("ip_dga_evidence").split("#")(1)
      val evidence = line.getAs[String]("ip_dga_evidence").split("#")(2)
      val sources: Array[String] = line.getAs[String]("sources").split("#")
      val positions: Array[String] = line.getAs[String]("positions").split("#")
      val count: Int = sources.length
      if (count >= countMin) {
        // 只保留该ip访问dga域名次数大于countMin
        (ip, dga, evidence, sources(0), count.toString, positions(0))
      } else {
        null
      }
    }.filter(_ != null)

    // 定义最终的结果输出
    var finalDF: DataFrame = null

    if (!tempRDD1.isEmpty()) {
      // rdd不能正确关联，所以转换为dataframe
      val tempDF1 = tempRDD1.toDF("ip", "dga", "evidence", "source", "count", "position")

      // 后期会关联这个dataframe，所以加入缓存
      tempDF1.persist()

      // 按ip聚合，相关信息一一对应，筛选出访问dga域名个数超过阈值的ip
      val tempRDD2: RDD[String] = tempDF1.select("ip", "dga").groupBy("ip").agg(concat_ws("#", collect_set("dga")))
        .withColumnRenamed("concat_ws(#, collect_set(dga))", "dgas").rdd.map { line =>
        val ip = line.getAs[String]("ip")
        val dga_nums: Int = line.getAs[String]("dgas").split("#").length
        if (dga_nums >= dgasMin) {
          ip
        } else {
          null
        }
      }.filter(_ != null)

      // 输出最终结果
      if (!tempRDD2.isEmpty()) {
        val tempDF2 = tempRDD2.toDF("abnormal_ip")

        // 关联ip，保存至最终结果
        finalDF = tempDF2.join(tempDF1, tempDF2("abnormal_ip") === tempDF1("ip"), "inner")
          .select("ip", "dga", "evidence", "source", "count", "position")
      }
      // 释放资源
      tempDF1.unpersist()
    }

    finalDF
  }

  //写文件
  def writeFile(dgaDF: DataFrame, dgaPath: String) = {
    val file_exist: Boolean = new File(dgaPath).exists()
    if (!file_exist) {
      // 新建该文件
      new File(dgaPath).createNewFile()
    }

    // 筛选新的dga域名
    import scala.io.Source
    val source: BufferedSource = Source.fromFile(dgaPath, "UTF-8")
    val local_dgasArr: Array[String] = source.getLines().toArray
    val new_dgasArr: Array[String] = dgaDF.rdd.map(_.getAs[String]("dga")).distinct().collect()
    val increased_dgasArr: Array[String] = new_dgasArr.diff(local_dgasArr)
    // 关闭连接
    source.close

    val increased_nums: Int = increased_dgasArr.size

    if (increased_nums > 0) {

      logger.error("@此次检测到的新DGA域名(计" + increased_nums + "个)>>>waiting...")
      val fileW = new FileWriter(dgaPath, true)
      increased_dgasArr.foreach {
        //写入数据
        line => fileW.write("\n" + line)
      }
      fileW.close()
      logger.error("写入数据成功>>>" + dgaPath)

    } else {
      logger.error("此次检测的dga域名在之前的文件中存在，不需要重复写入...")
    }


  }


  def makeKafkaDnsData(eventSourceDF: DataFrame, process_current_timeH: String): (Long, DataFrame) = {
    // eventSourceDF的字段("ip", "dga", "evidence", "source", "count", "position")
    import mysparksession.implicits._

    val kafkaDF = eventSourceDF.rdd.map {
      line =>
        val id: String = generateID
        // 网元ip
        val ip: String = line.getAs[String]("ip")
        // 事件类型
        val modeltype: String = "可疑域名"
        // 入库时间
        val resulttime: Long = getNowTime
        // dga域名的相关证据
        val evidence = line.getAs[String]("evidence")
        // 访问域名的五元组
        val eventsource: String = line.getAs[String]("source")
        // 是否造成主机沦陷0/1
        val happen: Int = 0
        // dga域名
        val normaltime: String = line.getAs[String]("dga")
        // 访问dga次数
        val abnormaltime: String = line.getAs[String]("count")
        // 访问dga的时间段
        val abnormalport: String = getDateTime(process_current_timeH)
        val abnormalproto = ""
        val abnormaltype = ""
        val hisbaseline = ""
        val standby01 = ""
        val standby02 = ""
        val standby03 = ""
        val event_rule_id = "MODEL_CCDGA_MV1.0_001_009"
        val proto = "dns"
        // 本机的ip，即上报ip
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_sub_type = "AttackBasic6B9"
        // 源ip和目的ip的地理位置信息
        val position: String = line.getAs[String]("position").replaceAll(",", "#")
        // 原始日志
        val original_log = "{\"网元IP\":\"" + ip + "\"," +
          "\"五元组（源IP，目的IP，源端口，目的端口，协议）\":\"" + eventsource + "\"," +
          "\"模型类型\":\"" + modeltype + "\"," +
          "\"发生时间（1h内）\":\"" + abnormalport + "" + "\"," +
          "\"上报IP\":\"" + reportneip + "\"," +
          "\"DGA域名\":\"" + normaltime + "\"," +
          "\"访问DGA域名的次数\":\"" + abnormaltime + "\"}"
        val encrypted = ""
        val event_flag = "0"
        val srcip = ip
        val dstip = eventsource.split(",")(1)
        val srcport = ""
        val dstport = ""

        eventRes(id, ip, modeltype, resulttime, evidence, eventsource, happen, normaltime, abnormaltime, abnormalport,
          abnormalproto, abnormaltype, hisbaseline, standby01, standby02, standby03, event_rule_id, proto, reportneip,
          event_sub_type, position, original_log, encrypted, event_flag, srcip, dstip, srcport, dstport)
    }.toDF()

    (kafkaDF.count(), kafkaDF)
  }


}

case class eventRes(id: String, ip: String, modeltype: String, resulttime: Long, evidence: String, eventsource: String,
                    happen: Int, normaltime: String, abnormaltime: String, abnormalport: String, abnormalproto: String,
                    abnormaltype: String, hisbaseline: String, standby01: String, standby02: String, standby03: String,
                    event_rule_id: String, proto: String, reportneip: String, event_sub_type: String, position: String,
                    original_log: String, encrypted: String, event_flag: String, srcip: String, dstip: String, srcport: String,
                    dstport: String)