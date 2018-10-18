package jobFiveMinute.subJob.ExternalConnection.ExternalConnectionClass

import java.io.Serializable
import java.net.InetAddress
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobFiveMinute.subClass.{LoggerSupport, saveToKAFKA}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{collect_set, concat_ws, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

import scala.util.Try

/**
  * Created by TTyb on 2018/1/13.
  */
class ExternalConnectionCheck(properties: Properties, spark: SparkSession, netData: DataFrame) extends LoggerSupport with Serializable with saveToKAFKA {
  def readyMain(): Unit = {
    //获取原始数据
    val netData = getData()
    netData.persist(StorageLevel.MEMORY_AND_DISK)
    //数据库中的加密端口协议信息
    val baseData = getDBData()
    //判断是否外联
    val (abConnection, noConnection) = baseOnIP(baseData, netData)
    val encryptedTunnelTrain = new ExternalConnectionTrain(properties, spark, noConnection)
    encryptedTunnelTrain.trainMain()

    //保存结果到数据库
    if (abConnection.take(10).length >= 1) {
      saveToKafka(abConnection, netData)
    }
    netData.unpersist()
  }

  // 获取数据
  def getData(): DataFrame = {
    netData.filter("flagIP=1")
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

  //读取数据库中的标记ip的数据
  def getDBData(): DataFrame = {
    logger_ec.error("获取数据库标记ip的数据")
    val lastTime = getDaytimeTime(-7)
    val nowTime = getDaytimeTime(1)
    val table = properties.getProperty("postgre.table.name.train")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select ip,typeresult,identityresult from $table where type='主动外联' and standby01 is not null and standby01 <> '' " +
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

    val ipMoment = baseData.groupBy("ip")
      .agg(concat_ws("#", collect_set("identityresult")))
      .withColumnRenamed("concat_ws(#, collect_set(identityresult))", "ipMoment")
    val completeData = baseData.join(ipMoment, Seq("ip"), "right_outer").filter("identityresult is not null")
    val codeMoment = (ipMoment: String) => {
      val momentArray = ipMoment.split("#")
      val ipmoments = momentArray.map(_.split("_").tail.head).mkString("#")
      ipmoments
    }
    val addColMoments = org.apache.spark.sql.functions.udf(codeMoment)
    val addColMomentDF = completeData.withColumn("ipmoments", addColMoments(completeData("ipMoment"))).drop("ipMoment")
    addColMomentDF
  }

  // 判断协议下的端口异常
  def baseOnIP(baseData: DataFrame, netflowData: DataFrame): (DataFrame, DataFrame) = {
    val table = properties.getProperty("postgre.table.name.train")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select min(CAST(standby01 AS BIGINT)) from $table where type='主动外联' and standby01 is not null and standby01 <> '') tt"
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

    val netData = addMoment(netflowData)
    val matchProto = netData.join(baseData, baseData("ip") === netData("srcip"), "outer")
    val code = (moment: String, ipmoments: String) => {
      var judge = 0
      if (ipmoments != null) {
        if (ipmoments.split("#").contains(moment)) {
          judge = 1
        }
      } else {
        judge = 1
      }
      if (lastTime < mtime) {
        judge = 1
      }
      judge
    }
    val addCol = udf(code)
    val externalConnection = matchProto.withColumn("judge", addCol(matchProto("moment"), matchProto("ipmoments")))
      .filter("srcip is not null")
      .dropDuplicates("srcip", "dstip", "moment")
      .distinct()
    val abConnection = externalConnection.filter("judge=0")
    val noConnection = externalConnection.filter("judge=1")
    (abConnection, noConnection)
  }

  //给原始数据增加时段
  def addMoment(dataFrame: DataFrame): DataFrame = {
    //提取recordtime中的小时段
    val code = (recordtime: String) => {

      val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val time: Date = fm.parse(fm.format(new Date(recordtime.toLong * 1000)))
      val cal = Calendar.getInstance()
      cal.setTime(time)
      //提取时间里面的小时时段
      val hour = cal.get(Calendar.HOUR_OF_DAY)
      /**
        * 早9:00-12:00
        * 中12:00-14:00
        * 下14:00-18:00
        * 晚18:00-24:00
        * 凌24:00-9:00
        */
      var moment = ""
      if (hour >= 9 && hour < 12) {
        moment = "早"
      } else if (hour >= 12 && hour < 14) {
        moment = "中"
      } else if (hour >= 14 && hour < 18) {
        moment = "下"
      } else if (hour >= 18 && hour < 24) {
        moment = "晚"
      } else if (hour >= 0 && hour < 9) {
        moment = "凌"
      }
      moment
    }

    val addCol = udf(code)
    val addDF = dataFrame.withColumn("moment", addCol(dataFrame("recordtime")))
    addDF
  }

  //保存结果到postgre
  def saveToKafka(externalIP: DataFrame, netData: DataFrame): Unit = {
    logger_ec.error("正在储存check结果")
    val dataFrame = addEventSource(externalIP)
    val nowTime = new Date().getTime.toString.substring(0, 10).toLong * 1000
    val netflowData = netData
      .select("flow_id", "proto7", "scountry", "sprovince", "scity", "slatitude", "slongitude", "dcountry", "dprovince",
        "dcity", "dlatitude", "dlongitude", "dstip", "srcport", "dstport","upbytesize")
    val newData = netflowData.join(dataFrame, Seq("flow_id"), "right_outer").filter("ip is not null").dropDuplicates("flow_id")
    import spark.implicits._
    val resultDF = newData.rdd.map {
      row =>
        val id = row.getAs[String]("id")
        val ip = row.getAs[String]("ip")
        val modelType = "主动外联"
        val time = nowTime
        val typeresult = replaceWord(row.getAs[String]("typeresult"))
        val moment = replaceWord(row.getAs[String]("moment"))
        val evidence = s"资产$ip 一般在$typeresult 时段外联，但是现在却在$moment 时段主动外联。"
        val eventSource = row.getAs[String]("eventSource").split("#").take(10).mkString("#")
        val happen: Int = 0
        val normaltime = replaceWord(row.getAs[String]("ipmoments"))
        val abnormaltime = moment
        val abnormalport = tranTime(nowTime)
        val abnormalproto = ""
        val abnormaltype = ""
        val hisbaseline = ""
        val standby01 = ""
        val standby02 = ""
        val standby03 = ""
        val event_rule_id = "MODEL_ABNORMALPORTS_MV1.0_001_006"
        val proto = ""
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_sub_type = "AttackBasic6B6"
        val position = row.getAs[String]("scountry") + "#" + row.getAs[String]("sprovince") + "#" + row.getAs[String]("scity") + "#" +
          row.getAs[String]("slatitude") + "#" + row.getAs[String]("slongitude") + "#" + row.getAs[String]("dcountry") + "#" +
          row.getAs[String]("dprovince") + "#" + row.getAs[String]("dcity") + "#" + row.getAs[String]("dlatitude") + "#" + row.getAs[String]("dlongitude")

        val original_log = "{\"源IP\":\"" + ip + "\"," +
          "\"目的IP\":\"" + row.getAs[String]("dstip") + "\"," +
          "\"源端口\":\"" + row.getAs[String]("srcport") + "\"," +
          "\"目的端口\":\"" + row.getAs[String]("dstport") + "\"," +
          "\"协议\":\"" + proto + "\"," +
          "\"模型类型\":\"" + "主动外联" + "\"," +
          "\"发生时间\":\"" + tranTimeToString(time) + "\"," +
          "\"上报IP\":\"" + reportneip + "\"," +
          "\"异常外联时段\":\"" + typeresult + "\"," +
          "\"正常外联时段\":\"" + replaceWord(row.getAs[String]("ipmoments")) + "\"}"
        var encrypted = "非加密流量"
        val proto7 = row.getAs[String]("proto7")
        if (proto7 == "tls" || proto7 == "TLS") {
          encrypted = "加密流量"
        }
        val event_flag = "0"
        val srcip = ip
        val dstip = row.getAs[String]("dstip")
        val srcport = row.getAs[String]("srcport")
        val dstport = row.getAs[String]("dstport")
        val upbytesize = row.getAs[String]("upbytesize")
        var flag = 0
        if (srcport.toInt > 1024 && dstport.toInt < 1024 && upbytesize.toInt > 1048576) {
          flag = 1
        }
        kafkaData(id, ip, modelType, time, evidence, eventSource, happen, normaltime, abnormaltime, abnormalport, abnormalproto,
          abnormaltype, hisbaseline, standby01, standby02, standby03, event_rule_id, proto, reportneip, event_sub_type, position,
          original_log, encrypted, event_flag, srcip, dstip, srcport, dstport, flag)
    }.toDS().toDF().filter("flag=1").drop("flag")
    if (resultDF.take(10).length != 0) {
      resultDF.show(2, false)
      val kafkaNodes = properties.getProperty("kafka.nodes")
      val kafkaPath = properties.getProperty("kafka.topic2")
      logger_ec.error(kafkaNodes)
      logger_ec.error(kafkaPath)
      toKafka(spark, (kafkaNodes, kafkaPath), resultDF)
    }
  }

  //增加溯源字段
  def addEventSource(encryptedPort: DataFrame): DataFrame = {
    logger_ec.error("正在抽取溯源信息")
    val eventSourceUdf = org.apache.spark.sql.functions.udf(
      (srcip: String, dstip: String, srcport: String, dstport: String, protocol: String) => {
        srcip + "," + dstip + "," + srcport + "," + dstport + "," + protocol
      }
    )
    val DF = encryptedPort.withColumn("eventSource", eventSourceUdf(encryptedPort("srcip"), encryptedPort("dstip"), encryptedPort("srcport"), encryptedPort("dstport"), encryptedPort("protocol")))
    val dataFrame = DF.groupBy("ip", "ipmoments", "moment", "typeresult", "flow_id")
      .agg(concat_ws("#", collect_set("eventSource")))
      .withColumnRenamed("concat_ws(#, collect_set(eventSource))", "eventSource")
    val IdUdf = org.apache.spark.sql.functions.udf(
      (dstip: String) => {
        ROWUtils.genaralROW()
      }
    )
    val rData = dataFrame.withColumn("id", IdUdf(dataFrame("ip")))
    rData.dropDuplicates("ip", "moment")
  }

  /**
    * 替换早中午晚凌中文
    * 早=早晨9:00-12:00
    * 中=中午12:00-14:00
    * 下=下午14:00-18:00
    * 晚=晚上18:00-24:00
    * 凌=凌晨0:00-9:00
    */
  def replaceWord(word: String): String = {
    val newWord = word
      .replace("早", "9:00-12:00")
      .replace("中", "12:00-14:00")
      .replace("下", "14:00-18:00")
      .replace("晚", "18:00-24:00")
      .replace("凌", "0:00-9:00")
    newWord
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
                     encrypted: String, event_flag: String, srcip: String, dstip: String, srcport: String, dstport: String, flag: Int)
