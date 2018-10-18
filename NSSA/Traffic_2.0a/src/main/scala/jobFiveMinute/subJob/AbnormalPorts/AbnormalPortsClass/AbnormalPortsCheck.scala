package jobFiveMinute.subJob.AbnormalPorts.AbnormalPortsClass

import java.io.Serializable
import java.net.InetAddress
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobFiveMinute.subClass.{LoggerSupport, saveToKAFKA}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{collect_set, concat_ws, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

import scala.util.Try

/**
  * Created by TTyb on 2018/1/13.
  */
class AbnormalPortsCheck(properties: Properties, spark: SparkSession, netData: DataFrame) extends LoggerSupport with Serializable with saveToKAFKA {
  def readyMain(): Unit = {
    //获取原始数据
    val netData = getData()
    netData.persist(StorageLevel.MEMORY_AND_DISK)
    //获取数据库中的基础端口数据
    val basePost = getBasePort()
    //判断目标ip是否异常
    val (abPorts, noPorts) = judgePort(netData, basePost)
    val abnormalPortsTrain = new AbnormalPortsTrain(properties, spark, noPorts)
    abnormalPortsTrain.trainMain()
    //异常端口数据放入到数据库中的函数
    saveToKafka(abPorts, basePost, netData)
    netData.unpersist()
  }

  // 获取数据
  def getData(): DataFrame = {
    //过滤1024以上的端口filterPort
    val DF = netData.filter("flagIP=2").filter("protocol='tcp' or protocol='udp'")
    val code = (SrcPort: String, DstPort: String) => {
      var judge = 0
      if (DstPort.toInt > SrcPort.toInt) {
        judge = 1
      }
      judge
    }
    val addCol = udf(code)
    val aimData = DF.withColumn("flag", addCol(DF("srcport"), DF("dstport")))
      .filter("flag=0")
      .drop("flag")
    filterPort(aimData)
  }

  //下载原始数据包时间转换
  def tranTime(nowTime: Long): String = {
    val fm = new SimpleDateFormat("HH:mm")
    val minutes: String = fm.format(new Date(nowTime))
    var times = ""
    if (minutes.takeRight(1).toInt >= 5) {
      times = fm.format(new Date(nowTime)).dropRight(1) + "0-" + fm.format(new Date(nowTime)).dropRight(1) + "5"
    } else {
      times = fm.format(new Date(nowTime - 300000)).dropRight(1) + "5-" + fm.format(new Date(nowTime)).dropRight(1) + "0"
    }
    times
  }

  //增加Info字段
  def addInfo(netData: DataFrame): DataFrame = {
    logger_ap.error("增加Info字段")
    val code = (DstIP: String, DstPort: String, Proto: String) => {
      DstIP + "_" + DstPort + "_" + Proto
    }
    val addCol = udf(code)
    val aimData = netData.withColumn("Info", addCol(netData("dstip"), netData("dstport"), netData("protocol")))
    aimData
  }

  // 获取库中原来的数据
  def getBasePort(): DataFrame = {
    logger_ap.error("获取数据库标记ip的数据")
    val lastTime = getDaytimeTime(-7)
    val nowTime = getDaytimeTime(1)
    val table = properties.getProperty("postgre.table.name.train")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select ip,dstport as port, type, baselineresult, identityresult as Info from $table where type='端口异常' and standby01 is not null and standby01 <> '' " +
      s"and CAST(standby01 AS BIGINT) >= $lastTime " +
      s"and CAST(standby01 AS BIGINT) <= $nowTime) tt"
    val baseData = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", address)
      .option("dbtable", getIPSql)
      .option("user", username)
      .option("password", password)
      .load()

    val ip_port_protocol = baseData.groupBy("ip")
      .agg(concat_ws("#", collect_set("Info")))
      .withColumnRenamed("concat_ws(#, collect_set(Info))", "ip_port_protocol")
    val completeData = baseData.join(ip_port_protocol, Seq("ip"), "right_outer").filter("baselineresult is not null")

    val codePort = (ip_port_protocol: String) => {
      val portArray = ip_port_protocol.split("#")
      val ipports = portArray.map(_.split("_").dropRight(1).tail.head).mkString("#")
      ipports
    }
    val codeProto = (ip_port_protocol: String) => {
      val portArray = ip_port_protocol.split("#")
      val ipprotocols = portArray.map(_.split("_").drop(1).tail.head).mkString("#")
      ipprotocols
    }
    val addColPort = org.apache.spark.sql.functions.udf(codePort)
    val addColProtocol = org.apache.spark.sql.functions.udf(codeProto)

    val addColPortDF = completeData.withColumn("ipports", addColPort(completeData("ip_port_protocol")))
    val addColProtocolDF = addColPortDF.withColumn("ipprotocols", addColProtocol(completeData("ip_port_protocol")))
    addColProtocolDF.drop("ip_port_protocol")
  }

  //判断目标ip是否异常
  def judgePort(netData: DataFrame, basePost: DataFrame): (DataFrame, DataFrame) = {
    logger_ap.error("判断目标ip是否端口异常")
    val table = properties.getProperty("postgre.table.name.train")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select min(CAST(standby01 AS BIGINT)) from $table where type='端口异常' and standby01 is not null and standby01 <> '') tt"
    val minTime = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", address)
      .option("dbtable", getIPSql)
      .option("user", username)
      .option("password", password)
      .load()

    val mtime = Try(minTime.rdd.map(_.toSeq.toList.head.toString).collect().toList.head.toLong).getOrElse(new Date().getTime.toString.substring(0, 10).toLong * 1000)
    val lastTime = getDaytimeTime(-7)

    val completeData = netData.join(basePost, netData("dstip") === basePost("ip"), "outer")
    val code = (ipports: String, ipprotocols: String, dstport: String, protocol: String) => {
      var judge = 0
      if (ipports != null) {
        if (ipports.split("#").contains(dstport) && ipprotocols.split("#").contains(protocol)) {
          judge = 1
        }
      } else {
        judge = 1
      }
      if (lastTime < mtime){
        judge = 1
      }
      judge
    }
    val addCol = udf(code)
    val abPorts = completeData.withColumn("judge", addCol(completeData("ipports"), completeData("ipprotocols"), completeData("dstport"), completeData("protocol")))
      .distinct()
    //如果judge = 1，则说明数据库包含了这个端口，如果judge = 0则说明数据库里面没有这个端口
    val noPorts = addInfo(abPorts.filter("judge=1").drop("info"))
    (abPorts.filter("judge=0"), noPorts)
  }

  //保存结果到postgre
  def saveToKafka(abPorts: DataFrame, basePost: DataFrame, netData: DataFrame): Unit = {
    logger_ap.error("正在储存结果")
    val nowTime = new Date().getTime.toString.substring(0, 10).toLong * 1000
    val dataFrame = addEventSource(abPorts, basePost)
    val netflowData = netData
      .select("flow_id", "proto7", "scountry", "sprovince", "scity", "slatitude", "slongitude", "dcountry", "dprovince",
        "dcity", "dlatitude", "dlongitude", "srcip", "srcport")
    val newData = netflowData.join(dataFrame, Seq("flow_id"), "right_outer").filter("ip is not null").dropDuplicates("ip")
    import spark.implicits._
    val resultDF = newData.rdd.map {
      row =>
        val id = row.getAs[String]("id")
        val ip = row.getAs[String]("dstip")
        val modelType = "端口异常"
        val time = nowTime
        val basePort = row.getAs[String]("port")
        val baseProto = row.getAs[String]("baselineresult")
        val netPort = row.getAs[String]("dstport")
        val netProto = row.getAs[String]("protocol")
        val evidence = s"资产$ip 通常以$basePort 端口、 $baseProto 协议传输流量，但本条流量却以以$netPort 端口、 $netProto 协议传输流量。"
        val eventSource = row.getAs[String]("eventSource").split("#").take(10).mkString("#")
        val happen: Int = 0
        val normalPort = row.getAs[String]("ipports")
        val normalProtocol = row.getAs[String]("ipprotocols")
        var abnormalType = ""
        if (normalPort.split("#").contains(netPort)) {
          abnormalType = "协议"
        } else if (normalProtocol.split("#").contains(netProto)) {
          abnormalType = "端口"
        }
        val hisbaseline = tranTime(nowTime)
        val standby01 = ""
        val standby02 = ""
        val standby03 = ""
        val event_rule_id = "MODEL_ABNORMALPORTS_MV1.0_001_007"
        val proto = row.getAs[String]("protocol")
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_sub_type = "AttackBasic6B7"
        val position = row.getAs[String]("scountry") + "#" + row.getAs[String]("sprovince") + "#" + row.getAs[String]("scity") + "#" +
          row.getAs[String]("slatitude") + "#" + row.getAs[String]("slongitude") + "#" + row.getAs[String]("dcountry") + "#" +
          row.getAs[String]("dprovince") + "#" + row.getAs[String]("dcity") + "#" + row.getAs[String]("dlatitude") + "#" + row.getAs[String]("dlongitude")
        val original_log = "{\"源IP\":\"" + row.getAs[String]("srcip") + "\"," +
          "\"目的IP\":\"" + ip + "\"," +
          "\"源端口\":\"" + row.getAs[String]("srcport") + "\"," +
          "\"目的端口\":\"" + netPort + "\"," +
          "\"协议\":\"" + proto + "\"," +
          "\"模型类型\":\"" + "端口异常" + "\"," +
          "\"发生时间\":\"" + tranTimeToString(time) + "\"," +
          "\"上报IP\":\"" + reportneip + "\"," +
          "\"异常端口\":\"" + netPort + "\"," +
          "\"异常协议\":\"" + netProto + "\"," +
          "\"开放端口\":\"" + basePort + "\"," +
          "\"开放协议\":\"" + baseProto + "\"}"
        var encrypted = "非加密流量"
        val proto7 = row.getAs[String]("proto7")
        if (proto7 == "tls" || proto7 == "TLS") {
          encrypted = "加密流量"
        }
        val event_flag = "0"
        val srcip = row.getAs[String]("srcip")
        val dstip = ip
        val srcport = row.getAs[String]("srcport")
        val dstport = netPort
        kafkaData(id, ip, modelType, time, evidence, eventSource, happen, normalPort, normalProtocol, netPort, netProto, abnormalType,
          hisbaseline, standby01, standby02, standby03, event_rule_id, proto, reportneip, event_sub_type, position, original_log,
          encrypted, event_flag, srcip, dstip, srcport, dstport)
    }.toDS().toDF()
    if (resultDF.take(10).length != 0) {
      resultDF.show(2,false)
      val kafkaNodes = properties.getProperty("kafka.nodes")
      val kafkaPath = properties.getProperty("kafka.topic2")
      logger_ap.error(kafkaNodes)
      logger_ap.error(kafkaPath)
      toKafka(spark, (kafkaNodes, kafkaPath), resultDF)
    }
  }

  //增加溯源字段
  def addEventSource(encryptedPort: DataFrame, basePost: DataFrame): DataFrame = {
    logger_ap.error("正在抽取溯源信息")
    val eventSourceUdf = org.apache.spark.sql.functions.udf(
      (srcip: String, dstip: String, srcport: String, dstport: String, protocol: String) => {
        srcip + "," + dstip + "," + srcport + "," + dstport + "," + protocol
      }
    )
    val DF = encryptedPort.withColumn("eventSource", eventSourceUdf(encryptedPort("srcip"), encryptedPort("dstip"), encryptedPort("srcport"), encryptedPort("dstport"), encryptedPort("protocol")))

    val dataFrame = DF.groupBy("dstip", "dstport", "protocol", "flow_id")
      .agg(concat_ws("#", collect_set("eventSource")))
      .withColumnRenamed("concat_ws(#, collect_set(eventSource))", "eventSource")
    val resultData = dataFrame.join(basePost, dataFrame("dstip") === basePost("ip"), "right_outer").filter("dstip is not null")

    val IdUdf = org.apache.spark.sql.functions.udf(
      (dstip: String) => {
        ROWUtils.genaralROW()
      }
    )
    val rData = resultData.withColumn("id", IdUdf(resultData("dstip")))
    rData.dropDuplicates("dstip", "dstport")
  }

  //过滤1024以上的端口
  def filterPort(dataFrame: DataFrame): DataFrame = {
    val code = (port: String) => {
      var flag = 0
      if (port.toInt < 1024 && port.toInt!= 80 && port.toInt!= 443 && port.toInt!= 22 && port.toInt!= 23 && port.toInt!= 139 && port.toInt!= 81) {
        flag = 1
      }
      flag
    }
    val judgePort = org.apache.spark.sql.functions.udf(code)
    val transData = dataFrame.withColumn("flag", judgePort(dataFrame("dstport")))
    transData.filter("flag=1").drop("flag")
  }

  //获取前7天的时间long类型
  def getDaytimeTime(day: Int): Long = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val newTime = fm.parse(fm.format(time))
    newTime.getTime
  }

  def tranTimeToString(tm:Long) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm))
    tim
  }
}

case class kafkaData(id: String, ip: String, modeltype: String, resulttime: Long, evidence: String, eventsource: String, happen: Int,
                     normaltime: String, abnormaltime: String, abnormalport: String, abnormalproto: String, abnormaltype: String,
                     hisbaseline: String, standby01: String, standby02: String, standby03: String, event_rule_id: String,
                     proto: String, reportneip: String, event_sub_type: String, position: String, original_log: String,
                     encrypted: String, event_flag: String, srcip: String, dstip: String, srcport: String, dstport: String)
