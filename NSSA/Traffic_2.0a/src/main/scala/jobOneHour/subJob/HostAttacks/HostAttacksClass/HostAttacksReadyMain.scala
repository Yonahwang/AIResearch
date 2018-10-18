package jobOneHour.subJob.HostAttacks.HostAttacksClass

import java.io.Serializable
import java.net.InetAddress
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobFiveMinute.subClass.{LoggerSupport, saveToKAFKA}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

/**
  * Created by TTyb on 2018/3/2.
  */
class HostAttacksReadyMain(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable with saveToKAFKA {

  def readyMain(): Unit = {
    val hostAttacksMergeData = new HostAttacksMergeData(properties, spark)
    val mData = hostAttacksMergeData.mergeData()
    //val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> "E:\\data\\hostattacks\\")
    //mData.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(dataOptions).save()

    //val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> "E:\\data\\hostattacks\\")
    //val mData = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
    if (mData.take(10).length != 0) {
      val nowTime = new Date().getTime.toString.substring(0, 10).toLong * 1000
      saveSample(mData, nowTime)
      val hostAttacksScore = new HostAttacksScore(properties, spark)
      val scoreData = hostAttacksScore.filterData(mData)
      val modelResultData = getTodayResult()
      saveToES(scoreData, modelResultData, nowTime)
    }
  }

  //保存样本到postgre
  def saveSample(mData: DataFrame, nowTime: Long): Unit = {
    logger.error("正在储存样本数据到postgre")
    mData.foreachPartition {
      part =>
        val conn_str = properties.getProperty("postgre.address")
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              val id = line.getAs[String]("id")
              val ip = line.getAs[String]("ip")
              val recordTime = nowTime
              val abnormalPorts = line.getAs[String]("abnormalPorts")
              val encryptedTunnel = line.getAs[String]("encryptedTunnel")
              val heartBeat = line.getAs[String]("heartBeat")
              val upDown = line.getAs[String]("upDown")
              val ccDga = line.getAs[String]("ccDga")
              val externalConnection = line.getAs[String]("externalConnection")
              val abnormalTraffic = line.getAs[String]("abnormalTraffic")
              val threatIntelligence = line.getAs[String]("threatIntelligence")
              val securityIncident = line.getAs[String]("securityIncident")
              val botNet = line.getAs[String]("botNet")
              val label = if (threatIntelligence == "1") "1" else "0"
              val flag = "0"

              val table = properties.getProperty("postgre.table.name.sample")
              val sqlText =
                s"""INSERT INTO $table (id, ip, recordtime, event_threat, event_detect_dga, event_traffic_anomaly,
                   | event_heartbeat, event_up_down, event_covert_channel, event_external_connection, event_abnormal_ports,
                   | event_botnet, event_security_log, label, flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              prep.setString(1, id)
              prep.setString(2, ip)
              prep.setTimestamp(3, new Timestamp(recordTime))
              prep.setString(4, threatIntelligence)
              prep.setString(5, ccDga)
              prep.setString(6, abnormalTraffic)
              prep.setString(7, heartBeat)
              prep.setString(8, upDown)
              prep.setString(9, encryptedTunnel)
              prep.setString(10, externalConnection)
              prep.setString(11, abnormalPorts)
              prep.setString(12, botNet)
              prep.setString(13, securityIncident)
              prep.setString(14, label)
              prep.setString(15, flag)
              prep.executeUpdate
            } catch {
              case e: Exception => logger.error("导入数据库出错" + e.getMessage)
            }
            finally {}
        }
        conn.close()
    }
  }

  //获取今天的主机沦陷数据
  def getTodayResult(): DataFrame = {
    val modelResult = properties.getProperty("modelResult")
    logger.error("获取今天的主机沦陷结果数据" + modelResult)
    //抓取时间范围
    val startTime = Timestamp.valueOf(getTime(0)).getTime
    val endTime = Timestamp.valueOf(getTime(1)).getTime
    val query = "{\"query\":{\"range\":{\"time\":{\"gte\":" + startTime + ",\"lte\":" + endTime + "}}}}"
    //抓取数据下来
    var modelResultData: DataFrame = null
    try {
      modelResultData = spark.esDF(modelResult, query).select("id", "ip").withColumnRenamed("id", "updateID")
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    modelResultData
  }

  //获取时间
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newTime: String = new SimpleDateFormat("yyyy-MM-dd" + " 00:00:00").format(time)
    newTime
  }

  def saveToES(resultData: DataFrame, modelResultData: DataFrame, nowTime: Long): Unit = {
    /**
      * sql注入0
      * 恶意代码注入1
      * 主机沦陷2
      * 僵尸网络3
      */
    import spark.implicits._
    val dataFrame = resultData.rdd.map {
      line =>
        val id = ROWUtils.genaralROW()
        val ip = line.getAs[String]("ip")
        val srcip = line.getAs[String]("ip")
        val dstip = line.getAs[String]("ip")
        val srcport = ""
        val dstport = ""
        val port = ""
        val proto = ""
        val resultType = "2"
        val time = nowTime.toString
        var typeresult = line.getAs[String]("eventSource")
        if (typeresult.take(1) == "@") {
          typeresult = typeresult.drop(1)
        }
        val judgmentresult = line.getAs[Int]("totalScores").toString
        val riskresult = line.getAs[String]("level").toString
        val calculategist = line.getAs[String]("dataTime")
        val calculateresult = ""
        val encryptedtunnel = line.getAs[String]("encryptedTunnelEvidence")
        val externalconnection = line.getAs[String]("externalConnectionEvidence")
        val abnormalports = line.getAs[String]("abnormalPortsEvidence")
        val abnormaltraffic = line.getAs[String]("abnormalTrafficEvidence")
        val ccdga = line.getAs[String]("ccDgaEvidence")
        val heartbeat = line.getAs[String]("heartBeatEvidence")
        val updown = line.getAs[String]("upDownEvidence")
        var standby01 = ""
        var standby02 = ""
        var standby03 = ""
        if (line.getAs[String]("event_threat") == "1") {
          standby01 = line.getAs[String]("event_threat")
        }
        if (line.getAs[String]("event_security_log") == "1") {
          standby02 = line.getAs[String]("event_security_log")
        }
        if (line.getAs[String]("event_botnet") == "1") {
          standby03 = line.getAs[String]("event_botnet")
        }
        val position = "#########"
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_rule_id = "MODEL_FALLHOST_MV1.0_001_003"
        val event_sub_type = "AttackBasic6B3"
        val attackflag: Int = 0


        modelResult(id, ip, srcip, dstip, srcport, dstport, port, proto, resultType, time, typeresult, judgmentresult, riskresult, calculategist,
          calculateresult, encryptedtunnel, externalconnection, abnormalports, abnormaltraffic, ccdga, heartbeat, updown,
          standby01, standby02, standby03, position, reportneip, event_rule_id, event_sub_type, attackflag)
    }.toDS().where("judgmentresult <> ''").toDF()
    //保存结果到ES索引tmodelresult
    if (dataFrame.take(10).length != 0) {
      dataFrame.show(5,false)
      val kafkaNodes = properties.getProperty("kafka.nodes")
      val kafkaPath = properties.getProperty("kafka.topic1")
      logger.error(kafkaNodes)
      logger.error(kafkaPath)
      toKafka(spark, (kafkaNodes, kafkaPath), dataFrame)
      //删除保存到postgresql
      val confDelete = properties.getProperty("confDelete")
      if (confDelete == "true") {
        deletePostgre()
      }
    }

  }

  def devideData(dataFrame: Dataset[modelResult], modelResultData: DataFrame): (DataFrame, DataFrame) = {
    var updateData: DataFrame = null
    var insertData: DataFrame = null
    if (modelResultData == null) {
      insertData = dataFrame.toDF()
    } else {
      updateData = dataFrame.join(modelResultData, Seq("ip"), "right_outer").filter("updateID is not null").drop("id").withColumnRenamed("updateID", "id")
      insertData = dataFrame.join(modelResultData, Seq("ip"), "left_outer").filter("updateID is null")
    }
    (updateData, insertData)
  }

  def deletePostgre(): Unit = {
    try {
      val conn_str = properties.getProperty("postgre.address")
      Class.forName("org.postgresql.Driver").newInstance
      val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
      val table = properties.getProperty("tmodelresult")
      val sqlText = s"""DELETE FROM $table """
      val prep = conn.prepareStatement(sqlText)
      prep.executeUpdate
    } catch {
      case e: Exception => logger.error("删除原来的数据出错" + e.getMessage)
    }
  }

}

case class modelResult(id: String, ip: String, srcip: String, dstip: String, srcport: String, dstport: String, port: String, proto: String,
                       resulttype: String, time: String, typeresult: String, riskresult: String, judgmentresult: String, calculategist: String,
                       calculateresult: String, encryptedtunnel: String, externalconnection: String, abnormalports: String,
                       abnormaltraffic: String, ccdga: String, heartbeat: String, updown: String, standby01: String, standby02: String,
                       standby03: String, position: String, reportneip: String, event_rule_id: String, event_sub_type: String, attackflag: Int)
