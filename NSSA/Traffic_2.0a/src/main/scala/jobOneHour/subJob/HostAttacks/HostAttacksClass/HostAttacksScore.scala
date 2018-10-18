package jobOneHour.subJob.HostAttacks.HostAttacksClass

import java.io.Serializable
import java.sql.Timestamp
import java.util.Properties
import jobOneHour.subClass.LoggerSupport
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by TTyb on 2018/3/5.
  */
class HostAttacksScore(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {

  def filterData(mData: DataFrame): DataFrame = {
    val scoreData = getScore(mData)
    scoreData.show(5,false)
    val tableSample = properties.getProperty("postgre.table.name.sample")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select * from $tableSample ) tt"
    logger.error("过滤白名单" + "tableSample")
    val sampleResultData = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", address)
      .option("dbtable", getIPSql)
      .option("user", username)
      .option("password", password)
      .load()
      .drop("id", "ip", "lable", "recordtime")
    val resultDF = scoreData.join(sampleResultData, Seq("event_abnormal_ports", "event_covert_channel", "event_heartbeat", "event_up_down", "event_detect_dga",
      "event_external_connection", "event_traffic_anomaly", "event_threat", "event_security_log", "event_botnet"), "right_outer")
      .where("id is not null and flag <> '2'").distinct()
    resultDF.dropDuplicates("ip")
  }

  def getScore(mData: DataFrame): DataFrame = {
    val confData = getConfData(mData)
    logger.error("计算默认模式和权重模式下的得分")
    import spark.implicits._
    val scoreData = confData.rdd.map {
      line =>
        //增加证明和溯源
        val abnormalPortsEvidence = line.getAs[String]("abnormalPortsEvidence")
        val encryptedTunnelEvidence = line.getAs[String]("encryptedTunnelEvidence")
        val heartBeatEvidence = line.getAs[String]("heartBeatEvidence")
        val upDownEvidence = line.getAs[String]("upDownEvidence")
        val ccDgaEvidence = line.getAs[String]("ccDgaEvidence")
        val externalConnectionEvidence = line.getAs[String]("externalConnectionEvidence")
        val abnormalTrafficEvidence = line.getAs[String]("abnormalTrafficEvidence")
        val eventSource = line.getAs[String]("eventSource")
        val dataTime = line.getAs[String]("dataTime")

        val id = line.getAs[String]("id")
        val ip = line.getAs[String]("ip")
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
        val event_threat = line.getAs[String]("event_threat")
        val event_detect_dga = line.getAs[String]("event_detect_dga")
        val event_traffic_anomaly = line.getAs[String]("event_traffic_anomaly")
        val event_heartbeat = line.getAs[String]("event_heartbeat")
        val event_up_down = line.getAs[String]("event_up_down")
        val event_covert_channel = line.getAs[String]("event_covert_channel")
        val event_external_connection = line.getAs[String]("event_external_connection")
        val event_abnormal_ports = line.getAs[String]("event_abnormal_ports")
        val event_botnet = line.getAs[String]("event_botnet")
        val event_security_log = line.getAs[String]("event_security_log")
        val weight_type = line.getAs[String]("weight_type")
        var totalScores = 0
        if (weight_type == "1") {
          totalScores = line.getAs[String]("scores").toDouble.toInt
        } else {
          totalScores = (100*(abnormalPorts.toInt * event_abnormal_ports.toFloat + encryptedTunnel.toInt * event_covert_channel.toFloat +
            heartBeat.toInt * event_heartbeat.toFloat + upDown.toInt * event_up_down.toFloat + ccDga.toInt * event_detect_dga.toFloat +
            externalConnection.toInt * event_external_connection.toFloat + abnormalTraffic.toInt * event_traffic_anomaly.toFloat +
            threatIntelligence.toInt * event_threat.toFloat + securityIncident.toInt * event_security_log.toFloat +
            botNet.toInt * event_botnet.toFloat)).toInt
        }
        if (threatIntelligence.toInt == 1 ){
          if (totalScores < 60){
            totalScores = 60
          }
        }
        (id, ip, abnormalPorts, encryptedTunnel, heartBeat, upDown, ccDga,
          externalConnection, abnormalTraffic, threatIntelligence, securityIncident, botNet, totalScores,
          eventSource, abnormalPortsEvidence, encryptedTunnelEvidence, heartBeatEvidence, upDownEvidence, ccDgaEvidence,
          externalConnectionEvidence, abnormalTrafficEvidence,dataTime)
    }.toDF("id", "ip", "event_abnormal_ports", "event_covert_channel", "event_heartbeat", "event_up_down", "event_detect_dga",
      "event_external_connection", "event_traffic_anomaly", "event_threat", "event_security_log", "event_botnet", "totalScores",
      "eventSource", "abnormalPortsEvidence", "encryptedTunnelEvidence","heartBeatEvidence", "upDownEvidence", "ccDgaEvidence",
      "externalConnectionEvidence", "abnormalTrafficEvidence","dataTime")

    val code = (score: Int) => {
      var level = ""
      if (score >= 0 && score < 60) {
        level = "无风险"
      }
      else if (score >= 60 && score < 70) {
        level = "低风险"
      }
      else if (score >= 70 && score < 80) {
        level = "中风险"
      }
      else if (score >= 80 && score < 100) {
        level = "高风险"
      } else if (score >= 100) {
        level = "高风险"
      } else {
        level = "无风险"
      }
      level
    }
    //风险等级
    val levelUdf = org.apache.spark.sql.functions.udf(code)
    val scoreline = properties.getProperty("scoreline").toInt
    val lastData = scoreData.withColumn("level", levelUdf(scoreData("totalScores"))).filter(s"totalScores >= $scoreline")
    lastData
  }

  def getConfData(mData: DataFrame): DataFrame = {

    val tableWeight = properties.getProperty("postgre.table.name.weight")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select * from $tableWeight ) tt"
    logger.error("获取最新的权重数据" + tableWeight)
    val weightSqlData = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", address)
      .option("dbtable", getIPSql)
      .option("user", username)
      .option("password", password)
      .load()
    //转换Timestamp到long类型
    val code = (arg1: Timestamp) => {
      arg1.getTime
    }
    val addCol = org.apache.spark.sql.functions.udf(code)
    val weightData = weightSqlData
      .withColumn("rtime", addCol(weightSqlData("recordtime")))
      .drop("id", "ip", "recordtime")
      .withColumnRenamed("rtime", "recordtime")

    //reduce获取特殊模式最新时间的数据
    import spark.implicits._
    val reduceMacthDF = weightData.rdd.map {
      line =>
        (line.getAs[String]("event_threat"), line.getAs[String]("event_detect_dga"), line.getAs[String]("event_traffic_anomaly"),
          line.getAs[String]("event_heartbeat"), line.getAs[String]("event_up_down"), line.getAs[String]("event_covert_channel"),
          line.getAs[String]("event_external_connection"), line.getAs[String]("event_abnormal_ports"), line.getAs[String]("event_botnet"),
          line.getAs[String]("event_security_log"), line.getAs[String]("weight_type"), line.getAs[Long]("recordtime"))
    }.keyBy(each => (each._1, each._2, each._3, each._4, each._5, each._6, each._7, each._8, each._9, each._10, each._11)).reduceByKey {
      (ele1, ele2) =>
        val result = if (ele1._2 > ele2._2)
          ele1
        else
          ele2
        result
    }.map(_._2).toDF("event_threat", "event_detect_dga", "event_traffic_anomaly", "event_heartbeat", "event_up_down"
      , "event_covert_channel", "event_external_connection", "event_abnormal_ports", "event_botnet", "event_security_log"
      , "weight_type", "recordtime")
    val newMacthData = weightData.join(reduceMacthDF, Seq("event_threat", "event_detect_dga", "event_traffic_anomaly", "event_heartbeat", "event_up_down"
      , "event_covert_channel", "event_external_connection", "event_abnormal_ports", "event_botnet", "event_security_log"
      , "weight_type", "recordtime"), "right_outer")
    val matchData = newMacthData.filter("weight_type='1'")

    //reduce获取默认模式最新时间的数据
    import spark.implicits._
    val reduceDefaultDF = weightData.rdd.map {
      line =>
        (line.getAs[String]("weight_type"), line.getAs[Long]("recordtime"))
    }.keyBy(_._1).reduceByKey {
      (ele1, ele2) =>
        val result = if (ele1._2 > ele2._2)
          ele1
        else
          ele2
        result
    }.map(_._2).toDF("weight_type", "recordtime")
    val newDefaultData = weightData.join(reduceDefaultDF, Seq("weight_type", "recordtime"), "right_outer")
    val defaultData = newDefaultData.filter("weight_type='0'").drop("weight_type", "recordtime", "scores")

    //填充null值
    val defaultMap = defaultData.collect().map(r => Map(defaultData.columns.zip(r.toSeq): _*)).head
    val matchedData = mData.join(matchData,
      mData("abnormalPorts") === matchData("event_abnormal_ports") &&
        mData("encryptedTunnel") === matchData("event_covert_channel") &&
        mData("heartBeat") === matchData("event_heartbeat") &&
        mData("upDown") === matchData("event_up_down") &&
        mData("ccDga") === matchData("event_detect_dga") &&
        mData("externalConnection") === matchData("event_external_connection") &&
        mData("abnormalTraffic") === matchData("event_traffic_anomaly") &&
        mData("threatIntelligence") === matchData("event_threat") &&
        mData("securityIncident") === matchData("event_security_log") &&
        mData("botNet") === matchData("event_botnet")
      , "full"
    ).where("id is not null")
    val defaultedData = matchedData.na.fill(defaultMap)
    defaultedData
  }
}
