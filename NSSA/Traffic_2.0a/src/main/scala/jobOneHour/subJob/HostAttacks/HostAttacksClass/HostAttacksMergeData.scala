package jobOneHour.subJob.HostAttacks.HostAttacksClass

import java.io.Serializable
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobOneHour.subClass.LoggerSupport
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

/**
  * Created by TTyb on 2018/3/2.
  */
class HostAttacksMergeData(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {

  def mergeData(): DataFrame = {
    logger.error("抽取多个表的数据开始合并")
    val dataFrame = getModelData()
    val IPset = dataFrame.select("ip", "evidence", "eventsource").distinct()
    val eventResultData = getEventData()
    val botResultData = getBotData()
    val eventResult = eventResultData.join(IPset, IPset("ip") === eventResultData("sourceip") or IPset("ip") === eventResultData("destip"), "right_outer")
    val code = (sourceip: String, destip: String) => {
      var flag = 1
      if (sourceip == null && destip == null) {
        flag = 0
      }
      flag
    }
    val addCol = org.apache.spark.sql.functions.udf(code)
    val eventResults = eventResult.withColumn("flag", addCol(eventResult("sourceip"), eventResult("destip"))).drop("sourceip", "destip").distinct()
    val botResult = botResultData.join(IPset, IPset("ip") === botResultData("botIP"), "right_outer").where("botIP is not null").drop("botIP").distinct()
    val mData = initDataFrame(dataFrame, eventResults, botResult)
    mData
  }

  //获取模型原始数据
  def getModelData(): DataFrame = {
    val tm = new Date().getTime
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tim = fm.format(new Date(tm))
    val tableModel = properties.getProperty("hostAttacks") + "_" + tim + "/" + properties.getProperty("hostAttacks") + "_" + tim
    logger.error("获取模型结果数据" + tableModel)

    //抓取数据下来
    val rowDF = spark.esDF(tableModel, "").select("id", "ip", "modeltype", "resulttime", "evidence", "eventsource", "hisbaseline", "normaltime", "abnormalport", "abnormaltype", "standby01", "standby02")
    //选择时间最大的事件
    import spark.implicits._
    val reduceDF = rowDF.rdd.map {
      line =>
        (line.getAs[String]("ip"), line.getAs[String]("modeltype"), line.getAs[Long]("resulttime"))
    }.keyBy(each => (each._1, each._2)).reduceByKey {
      (ele1, ele2) =>
        val result = if (ele1._2 > ele2._2)
          ele1
        else
          ele2
        result
    }.map(_._2).toDF("ip", "modeltype", "resulttime")
    val dataFrame = rowDF.join(reduceDF, Seq("ip", "modeltype", "resulttime"), "right_outer").filter("id is not null").dropDuplicates("ip", "modeltype")
    dataFrame
  }

  //获取威胁情报、安全事件结果
  def getEventData(): DataFrame = {
    var eventResultData: DataFrame = spark.createDataFrame(Seq(
      ("127.0.0.1", "127.0.0.1", "0")
    )).toDF("sourceip", "destip", "is_threat")

    try {
      val tm = new Date().getTime
      val fm = new SimpleDateFormat("yyyyMMdd")
      val tim = fm.format(new Date(tm))
      val tableEvent = properties.getProperty("postgre.table.name.event") + "_" + tim
      val address = properties.getProperty("postgre.address")
      val username = properties.getProperty("postgre.user")
      val password = properties.getProperty("postgre.password")
      val getIPSql = s"(select sourceip,destip,is_threat from $tableEvent ) tt"
      logger.error("获取事件结果表的数据" + tableEvent)
      eventResultData = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", address)
        .option("dbtable", getIPSql)
        .option("user", username)
        .option("password", password)
        .load()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    eventResultData
  }

  //获取僵尸网络结果
  def getBotData(): DataFrame = {
    var botResultData: DataFrame = spark.createDataFrame(Seq(
      ("127.0.0.2", "127.0.0.1")
    )).toDF("botIP", "temp1").drop("temp1")
    try {
      //抓取时间范围
      val startTime = Timestamp.valueOf(getTime(0)).getTime
      val endTime = Timestamp.valueOf(getTime(1)).getTime
      val query = "{\"query\":{\"range\":{\"recordtime\":{\"gte\":" + startTime + ",\"lte\":" + endTime + "}}}}"
      val tableBot = properties.getProperty("postgre.table.name.bot")
      logger.error("获取僵尸网络结果表的数据" + tableBot)
      botResultData = spark.esDF(tableBot, query).select("ip").withColumnRenamed("ip", "botIP").distinct()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    botResultData
  }

  //获取时间
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newTime: String = new SimpleDateFormat("yyyy-MM-dd" + " 00:00:00").format(time)
    newTime
  }


  def tranTime(tm: String, format: String): String = {
    //字符串时间转换时间戳
    val fm = new SimpleDateFormat(format)
    val dt = fm.parse(tm)
    val tim: Long = dt.getTime()
    //时间戳转化字符串时间
    val newFm = new SimpleDateFormat("HH:mm")
    val startTim = newFm.format(new Date(tim))
    val endTim = newFm.format(new Date(tim + 3600000))
    val newTim = startTim.toString + "-" + endTim.toString
    newTim
  }

  /**
    * 格式化dataFrame
    * abnormalPorts 端口异常
    * encryptedTunnel 加密通道
    * externalConnection 主动外联
    * ccDga 可疑域名
    * heartBeat 间歇会话连接
    * abnormalTraffic 异常流量
    * upDown 上下行流量异常
    */
  def initDataFrame(modelData: DataFrame, eventResults: DataFrame, botResult: DataFrame): DataFrame = {
    logger.error("格式化结果数据")
    //端口异常
    val abnormalPorts = modelData
      .where("modeltype='端口异常'")
      .select("ip", "evidence", "eventsource", "hisbaseline")
      .withColumnRenamed("ip", "abnormalPorts")
      .withColumnRenamed("evidence", "abnormalPortsEvidence")
      .withColumnRenamed("eventsource", "abnormalPortsEventSource")
      .withColumnRenamed("hisbaseline", "abnormalPortsTime")
    //加密通道
    val encryptedTunnel = modelData
      .where("modeltype='隐蔽通道'")
      .select("ip", "evidence", "eventsource", "normaltime")
      .withColumnRenamed("ip", "encryptedTunnel")
      .withColumnRenamed("evidence", "encryptedTunnelEvidence")
      .withColumnRenamed("eventsource", "encryptedTunnelEventSource")
      .withColumnRenamed("normaltime", "encryptedTunnelTime")
    //主动外联
    val externalConnection = modelData
      .where("modeltype='主动外联'")
      .select("ip", "evidence", "eventsource", "abnormalport")
      .withColumnRenamed("ip", "externalConnection")
      .withColumnRenamed("evidence", "externalConnectionEvidence")
      .withColumnRenamed("eventsource", "externalConnectionEventSource")
      .withColumnRenamed("abnormalport", "externalConnectionTime")
    //可疑域名
    val ccDga = modelData
      .where("modeltype='可疑域名'")
      .select("ip", "evidence", "eventsource", "abnormalport")
      .withColumnRenamed("ip", "ccDga")
      .withColumnRenamed("evidence", "ccDgaEvidence")
      .withColumnRenamed("eventsource", "ccDgaEventSource")
      .withColumnRenamed("abnormalport", "ccDgaTime")
    //间歇会话连接
    /**
      * 间歇会话连接   standby01  是可疑域名的标识，为“2”的时候就是匹配中可疑域名，standby02是平均上行流量大小
      *
      **/
    val heartBeat = modelData
      .where("modeltype='间歇会话连接'")
      .select("ip", "evidence", "eventsource", "abnormaltype", "standby01", "standby02")
      .filter("standby01 = '2'")
      .filter("CAST(standby02 AS BIGINT) < 70")
      .withColumnRenamed("ip", "heartBeat")
      .withColumnRenamed("evidence", "heartBeatEvidence")
      .withColumnRenamed("eventsource", "heartBeatEventSource")
      .withColumnRenamed("abnormaltype", "heartBeatTime")
      .drop("standby01", "standby02")
    //异常流量
    val abnormalTraffic = modelData
      .where("modeltype='异常流量'")
      .select("ip", "evidence", "eventsource", "abnormalport")
      .withColumnRenamed("ip", "abnormalTraffic")
      .withColumnRenamed("evidence", "abnormalTrafficEvidence")
      .withColumnRenamed("eventsource", "abnormalTrafficEventSource")
      .withColumnRenamed("abnormalport", "abnormalTrafficTime")
    //上下行流量异常
    /**
      * 上下行异常流量 standby02 为“1”标识匹配中当天的可疑域名
      *
      **/
    val upDown = modelData
      .where("modeltype='上下行流量异常'")
      .select("ip", "evidence", "eventsource", "standby02")
      .filter("standby02='1'")
      .withColumnRenamed("ip", "upDown")
      .withColumnRenamed("evidence", "upDownEvidence")
      .withColumnRenamed("eventsource", "upDownEventSource")
      .withColumnRenamed("standby02", "upDownTime")
    //威胁情报
    val threatIntelligence = eventResults
      .where("is_threat=1 and flag=1")
      .select("ip")
      .withColumnRenamed("ip", "threatIntelligence")
    //安全事件
    val securityIncident = eventResults
      .where("flag=1")
      .select("ip")
      .withColumnRenamed("ip", "securityIncident")
    //僵尸网络
    val botNet = botResult
      .select("ip")
      .withColumnRenamed("ip", "botNet")

    val udata = abnormalPorts.join(encryptedTunnel, abnormalPorts("abnormalPorts") === encryptedTunnel("encryptedTunnel"), "full")
    val udata1 = udata.join(heartBeat, udata("abnormalPorts") === heartBeat("heartBeat") or udata("encryptedTunnel") === heartBeat("heartBeat"), "full")
    val udata2 = udata1.join(upDown, udata1("abnormalPorts") === upDown("upDown") or udata1("encryptedTunnel") === upDown("upDown") or udata1("heartBeat") === upDown("upDown"), "full")
    val udata3 = udata2.join(ccDga, udata2("abnormalPorts") === ccDga("ccDga") or udata2("encryptedTunnel") === ccDga("ccDga") or udata2("heartBeat") === ccDga("ccDga") or udata2("upDown") === ccDga("ccDga"), "full")
    val udata4 = udata3.join(externalConnection, udata3("abnormalPorts") === externalConnection("externalConnection") or udata3("encryptedTunnel") === externalConnection("externalConnection") or udata3("heartBeat") === externalConnection("externalConnection") or udata3("upDown") === externalConnection("externalConnection") or udata3("ccDga") === externalConnection("externalConnection"), "full")
    val udata5 = udata4.join(abnormalTraffic, udata4("abnormalPorts") === abnormalTraffic("abnormalTraffic") or udata4("encryptedTunnel") === abnormalTraffic("abnormalTraffic") or udata4("heartBeat") === abnormalTraffic("abnormalTraffic") or udata4("upDown") === abnormalTraffic("abnormalTraffic") or udata4("ccDga") === abnormalTraffic("abnormalTraffic") or udata4("externalConnection") === abnormalTraffic("abnormalTraffic"), "full")
    val udata6 = udata5.join(threatIntelligence, udata5("abnormalPorts") === threatIntelligence("threatIntelligence") or udata5("encryptedTunnel") === threatIntelligence("threatIntelligence") or udata5("heartBeat") === threatIntelligence("threatIntelligence") or udata5("upDown") === threatIntelligence("threatIntelligence") or udata5("ccDga") === threatIntelligence("threatIntelligence") or udata5("externalConnection") === threatIntelligence("threatIntelligence") or udata5("abnormalTraffic") === threatIntelligence("threatIntelligence"), "full")
    val udata7 = udata6.join(securityIncident, udata6("abnormalPorts") === securityIncident("securityIncident") or udata6("encryptedTunnel") === securityIncident("securityIncident") or udata6("heartBeat") === securityIncident("securityIncident") or udata6("upDown") === securityIncident("securityIncident") or udata6("ccDga") === securityIncident("securityIncident") or udata6("externalConnection") === securityIncident("securityIncident") or udata6("abnormalTraffic") === securityIncident("securityIncident") or udata6("threatIntelligence") === securityIncident("securityIncident"), "full")
    val initData = udata7.join(botNet, udata7("abnormalPorts") === botNet("botNet") or udata7("encryptedTunnel") === botNet("botNet") or udata7("heartBeat") === botNet("botNet") or udata7("upDown") === botNet("botNet") or udata7("ccDga") === botNet("botNet") or udata7("externalConnection") === botNet("botNet") or udata7("abnormalTraffic") === botNet("botNet") or udata7("threatIntelligence") === botNet("botNet") or udata7("securityIncident") === botNet("botNet"), "full")

    import spark.implicits._
    val mData = initData.rdd.map {
      line =>
        //增加证明和溯源
        val abnormalPortsEvidence = line.getAs[String]("abnormalPortsEvidence")
        val abnormalPortsEventSource = line.getAs[String]("abnormalPortsEventSource")
        val encryptedTunnelEvidence = line.getAs[String]("encryptedTunnelEvidence")
        val encryptedTunnelEventSource = line.getAs[String]("encryptedTunnelEventSource")
        val heartBeatEvidence = line.getAs[String]("heartBeatEvidence")
        val heartBeatEventSource = line.getAs[String]("heartBeatEventSource")
        val upDownEvidence = line.getAs[String]("upDownEvidence")
        val upDownEventSource = line.getAs[String]("upDownEventSource")
        val ccDgaEvidence = line.getAs[String]("ccDgaEvidence")
        val ccDgaEventSource = line.getAs[String]("ccDgaEventSource")
        val externalConnectionEvidence = line.getAs[String]("externalConnectionEvidence")
        val externalConnectionEventSource = line.getAs[String]("externalConnectionEventSource")
        val abnormalTrafficEvidence = line.getAs[String]("abnormalTrafficEvidence")
        val abnormalTrafficEventSource = line.getAs[String]("abnormalTrafficEventSource")
        var eventSource = ""

        //下面是加个时间给原始流量包下载
        val abnormalPortsTime = if (line.getAs[String]("abnormalPortsTime") == null) "" else line.getAs[String]("abnormalPortsTime")
        val encryptedTunnelTime = if (line.getAs[String]("encryptedTunnelTime") == null) "" else tranTime(line.getAs[String]("encryptedTunnelTime"), "yy-MM-dd hh:mm")
        val externalConnectionTime = if (line.getAs[String]("externalConnectionTime") == null) "" else line.getAs[String]("externalConnectionTime")

        var ccDgaTime = ""
        if (line.getAs[String]("ccDgaTime") != null) {
          val ccDgaTimeST = line.getAs[String]("ccDgaTime").toLong
          val newFm = new SimpleDateFormat("HH:mm")
          ccDgaTime = newFm.format(new Date(ccDgaTimeST)) + "-" + newFm.format(new Date(ccDgaTimeST + 3600000))
        }
        val heartBeatTime = if (line.getAs[String]("heartBeatTime") == null) "" else tranTime(line.getAs[String]("heartBeatTime"), "yy-MM-dd-hh:mm")

        var abnormalTrafficTime = ""
        if (line.getAs[String]("abnormalTrafficTime") != null) {
          val abnormalTrafficTimeST = line.getAs[String]("abnormalTrafficTime").toLong
          val newFm = new SimpleDateFormat("HH:mm")
          abnormalTrafficTime = newFm.format(new Date(abnormalTrafficTimeST)) + "-" + newFm.format(new Date(abnormalTrafficTimeST + 3600000))
        }
        val upDownTime = if (line.getAs[String]("upDownTime") == null) "" else line.getAs[String]("upDownTime")
        val dataTime = abnormalPortsTime + "#" + encryptedTunnelTime + "#" + externalConnectionTime + "#" + ccDgaTime +
          "#" + heartBeatTime + "#" + abnormalTrafficTime + "#" + upDownTime

        //判断事件
        val id = ROWUtils.genaralROW()
        var ip = ""
        val abnormalPorts = if (line.getAs[String]("abnormalPorts") == null) "0" else "1"
        val encryptedTunnel = if (line.getAs[String]("encryptedTunnel") == null) "0" else "1"
        val heartBeat = if (line.getAs[String]("heartBeat") == null) "0" else "1"
        val upDown = if (line.getAs[String]("upDown") == null) "0" else "1"
        val ccDga = if (line.getAs[String]("ccDga") == null) "0" else "1"
        val externalConnection = if (line.getAs[String]("externalConnection") == null) "0" else "1"
        val abnormalTraffic = if (line.getAs[String]("abnormalTraffic") == null) "0" else "1"
        val threatIntelligence = if (line.getAs[String]("threatIntelligence") == null) "0" else "1"
        val securityIncident = if (line.getAs[String]("securityIncident") == null) "0" else "1"
        val botNet = if (line.getAs[String]("botNet") == null) "0" else "1"

        if (abnormalPorts == "1") {
          ip = line.getAs[String]("abnormalPorts")
          eventSource = eventSource + "@" + "异常端口：" + abnormalPortsEventSource
        }
        if (encryptedTunnel == "1") {
          ip = line.getAs[String]("encryptedTunnel")
          eventSource = eventSource + "@" + "隐蔽通道：" + encryptedTunnelEventSource
        }
        if (heartBeat == "1") {
          ip = line.getAs[String]("heartBeat")
          eventSource = eventSource + "@" + "间歇会话连接：" + heartBeatEventSource
        }
        if (upDown == "1") {
          ip = line.getAs[String]("upDown")
          eventSource = eventSource + "@" + "上下行流量异常：" + upDownEventSource
        }
        if (ccDga == "1") {
          ip = line.getAs[String]("ccDga")
          eventSource = eventSource + "@" + "可疑域名：" + ccDgaEventSource
        }
        if (externalConnection == "1") {
          ip = line.getAs[String]("externalConnection")
          eventSource = eventSource + "@" + "主动外联：" + externalConnectionEventSource
        }
        if (abnormalTraffic == "1") {
          ip = line.getAs[String]("abnormalTraffic")
          eventSource = eventSource + "@" + "异常流量：" + abnormalTrafficEventSource
        }
        if (threatIntelligence == "1") ip = line.getAs[String]("threatIntelligence")
        if (securityIncident == "1") ip = line.getAs[String]("securityIncident")
        if (botNet == "1") ip = line.getAs[String]("botNet")

        (id, ip, abnormalPorts, encryptedTunnel, heartBeat, upDown, ccDga, externalConnection, abnormalTraffic, threatIntelligence,
          securityIncident, botNet, eventSource, abnormalPortsEvidence, encryptedTunnelEvidence, heartBeatEvidence, upDownEvidence,
          ccDgaEvidence, externalConnectionEvidence, abnormalTrafficEvidence, dataTime)
    }.toDF("id", "ip", "abnormalPorts", "encryptedTunnel", "heartBeat", "upDown", "ccDga", "externalConnection", "abnormalTraffic",
      "threatIntelligence", "securityIncident", "botNet", "eventSource", "abnormalPortsEvidence", "encryptedTunnelEvidence",
      "heartBeatEvidence", "upDownEvidence", "ccDgaEvidence", "externalConnectionEvidence", "abnormalTrafficEvidence", "dataTime")
    mData
  }
}
