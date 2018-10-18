package jobOneDay.subJob.NetflowBotNet.NetflowBotNetClass

import java.io.Serializable
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

import scala.collection.mutable.ArrayBuffer
import jobOneDay.subClass.{LoggerSupport,saveToKAFKA}

/**
  * Created by TTyb on 2018/2/2.
  */
class NetflowBotNetReadyMain(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable with saveToKAFKA {
  def readyMain(): Unit = {
    val correlateDF = correlateData()
    val netflowBotNetJudge = new NetflowBotNetJudge(properties, spark)
    val resultDF = netflowBotNetJudge.judgeBotNet(correlateDF.select("flow_id", "srcip", "srcport", "dstport", "dstip", "netprotocol", "downbytesize", "upbytesize", "recordtime", "answers_data_length", "MaliciousDomain"))
    resultDF.show(false)
    saveToKafka(resultDF)
  }

  //获取恶意域名数据
  def getMaliciousDomain(): DataFrame = {
    import spark.implicits._
    val path = properties.getProperty("malicious.domain")
    val maliciousDomain = spark.sparkContext.textFile(path).toDF("MaliciousDomain").distinct().na.drop()
    maliciousDomain
  }

  //获取文件的名字
  def getFileName(filePath: String, index: String): Array[String] = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd0000")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    val nowTime = new Date()
    val nowDay = dateFormat.format(nowTime)
    logger.error(filePath + ":" + yesterday + "-" + nowDay)

    //获取前缀后缀
    var prefix = ""
    var suffix = ""

    var arrayFile: Array[String] = new ArrayBuffer[String]().toArray

    val configuration = new Configuration()
    val output = new Path(filePath)
    val hdfs = output.getFileSystem(configuration)
    val fs = hdfs.listStatus(output)
    val fileName = FileUtil.stat2Paths(fs)
    hdfs.close()

    fileName.foreach { eachfile =>
      val eachFileName = eachfile.getName.split("\\.")
      prefix = eachFileName.head.replace(filePath, "").replace(index, "")
      suffix = eachFileName.last
      if (prefix.toLong >= yesterday.toLong && prefix.toLong < nowDay.toLong) {
        arrayFile = arrayFile :+ filePath + "/" + prefix.toLong.toString + "." + suffix
      }
    }
    arrayFile
  }

  //读取数据
  def getData(PrepareFilePath: String): DataFrame = {
    logger.error("获取预处理后的数据数据" + PrepareFilePath)
    val hdfsUrl = properties.getProperty("hdfsUrl")
    val arrayFile = getFileName(hdfsUrl + PrepareFilePath, "netflow")
    //读取dataframe
    val dataFrameList: List[DataFrame] = arrayFile.map {
      path =>
        logger.error(path)
        var result: DataFrame = null
        try {
          val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
          result = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
        } catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }
        result
    }.toList.filter(_ != null)

    //合并dataframe
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val unionData = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    unionData
  }

  //将可疑域名、DNS、HTTP、NETFLOW的数据关联起来
  def correlateData(): DataFrame = {
    val maliciousDomain = getMaliciousDomain()
    //预处理后数据的路径
    val NetPrepareFilePath: String = properties.getProperty("netPrepareHDFS")
    val DnsPrepareFilePath: String = properties.getProperty("DNSPrepareHDFS")
    val dnsData = getData(DnsPrepareFilePath)
    val netData = getData(NetPrepareFilePath)
    logger.error("关联dns、netflow数据")
    val dnsBotNet = dnsData.join(maliciousDomain, dnsData("queries_name") === maliciousDomain("MaliciousDomain"), "right_outer").where("answers_address is not null and flow_id is not null").drop("flags_reply_code", "flagIP", "dstip").withColumnRenamed("protocol", "dnsprotocol")
    val netDnsBotNetNet = netData.join(dnsBotNet, Seq("flow_id", "srcip", "srcport", "dstport"), "right_outer").where("protocol='tcp' or protocol='udp'").withColumnRenamed("protocol", "netprotocol").drop("flagIP", "recordtime").withColumnRenamed("timestamp", "recordtime")
    netDnsBotNetNet
  }

  //储存结果到ES中
  def saveToKafka(dataFrame: DataFrame): Unit = {
    //增加lable
    val indexer = new StringIndexer()
      .setInputCol("MaliciousDomain")
      .setOutputCol("hostLable")

    val indexDF = indexer.fit(dataFrame).transform(dataFrame)
    //失陷资产的个数
    val attacksHost = dataFrame.filter(dataFrame("ip").isNotNull).count().toDouble
    val allHost = dataFrame.count().toDouble
    import spark.implicits._
    val resultDF = indexDF.rdd.map {
      row =>
        val id = ROWUtils.genaralROW()
        val ip = row.getAs[String]("srcip")
        val nowTime = new Date().getTime.toString.substring(0, 10).toLong * 1000
        var status = "1"
        val statusIP = row.getAs[String]("ip")
        if (statusIP != null) {
          status = "0"
        }
        val result_type = "1"
        //属于哪一组僵尸网络
        val hostLable = row.getAs[Double]("hostLable").toInt
        val domain_name = row.getAs[String]("MaliciousDomain")
        //间歇性会话的时间段
        val session_time = row.getAs[String]("abnormaltype")
        //间歇性会话的次数
        val session_count = row.getAs[String]("abnormalproto")
        // 主机所在类别是僵尸网络的概率
        val botProbability = (attacksHost / allHost).toString
        val content = (row.getAs[String]("downbytesize").toInt + row.getAs[String]("upbytesize").toInt).toString
        val content_count = row.getAs[String]("answers_data_length").split("#").map(_.toInt).sum.toString
        val stringTime = row.getAs[String]("stringTime").split("#").map(x => x.toLong)
        val content_confusion = stringTime.min.toString + "#" + stringTime.max.toString
        val field01 = ""
        val field02 = ""
        val field03 = ""
        (id, ip, nowTime, status, result_type, hostLable, domain_name, session_time, session_count, botProbability, content, content_count, content_confusion, field01, field02, field03)
    }.toDF("id", "ip", "recordtime", "status", "result_type", "label", "domain_name", "session_time", "session_count", "botnet_probability", "content", "content_count", "content_confusion", "field01", "field02", "field03")
    if (resultDF.take(10).length != 0) {
      val kafkaNodes = properties.getProperty("kafka.nodes")
      val kafkaPath = properties.getProperty("kafka.topic3")
      logger.error(kafkaNodes)
      logger.error(kafkaPath)
      toKafka(spark, (kafkaNodes, kafkaPath), resultDF)
    }
  }
}
