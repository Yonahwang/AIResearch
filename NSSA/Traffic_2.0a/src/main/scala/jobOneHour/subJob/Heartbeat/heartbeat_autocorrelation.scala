package jobOneHour.subJob.Heartbeat

import java.io.Serializable
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import jobOneHour.subClass.saveToKAFKA
import org.apache.spark.sql.functions.{collect_set, concat_ws, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.spark.sql._
import jobOneHour.subClass._

class heartbeat_autocorrelation(spark: SparkSession,data:DataFrame,properties:Properties) extends Serializable with LoggerSupport with saveToKAFKA{
  val first = 	getfirstTimeIndex()-100
  val last =   getfirstTimeIndex()

  /**
    * 对流量计算周期性连接，匹配dga域名和威胁情报
    * 结果表
    * 源ip  目的ip  目的端口  协议  是否命中dga/威胁情报  访问次数   时间段
    *
    */

  def heartbeatPredict() = {
    try {
      logger.error("<<<<<<<<<<<<<<<<<<<<<<<间歇会话检测任务开始于" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + ">>>>>>>>>>>>>>>>>>>>>>")

      //读取es访问过可疑域名的表
      val domaintablename = "thostattacks_" + getEStableIndex()+ "/thostattacks_" +getEStableIndex()
      val domaindata = getDataFromES(domaintablename, spark).filter("modeltype == '可疑域名'").select("ip").distinct()

      //读取威胁情报的数据

      val threadtablename = "t_siem_event_result_" + getEStableIndex()
      val threaddata = getDataFromPostgre(spark, properties.getProperty("postgre.address"), properties.getProperty("postgre.user"), properties.getProperty("postgre.password"), threadtablename).select("destip").distinct().toDF("ip")

      //处理数据,生成循环自相关系数
      val dealData = dealdata(spark, data, domaindata, threaddata)
      dealData.show(3, false)
      //存kafka
      toKafka(spark,(properties.getProperty("kafka.nodes"), properties.getProperty("kafka.topic2")),dealData)
    }catch {
      case e: EsHadoopIllegalArgumentException => println(e.getMessage)
      case e:Exception => println(e.getMessage)
    }
  }


  //存结果表,构建临时表
  def dealdata(spark: SparkSession, data: DataFrame, domaindata: DataFrame, threaddata: DataFrame): DataFrame = {
    /**
      * 过滤出小数据包，内网到外网的数据流，
      *
      */
    //将未知流量过滤出netflow，内网到外网，以recordtime排序，小数据包
    logger.error("过滤数据")

    val fdata = data.filter("flagIP == '1'")
    //合并"srcip", "dstip", "dstport", "protocol", "proto7"  作为唯一标识
    val hbudf = udf((srcip: String, dstip: String, dstport: String, protocol: String,proto7:String) => (srcip + "+" + dstip + "+" + dstport + "+" + protocol+"+"+proto7))
    val hbdata = fdata.withColumn("srcdst", hbudf(fdata("srcip"), fdata("dstip"), fdata("dstport"), fdata("protocol"), fdata("proto7")))
    hbdata.printSchema()


    val filterdomaindata = hbdata
      .select("srcdst", "recordtime","upbytesize")
      .na.drop(Array("srcdst", "recordtime","upbytesize"))
      .distinct().orderBy("recordtime")

    //整合数据包的大小

    filterdomaindata.createOrReplaceTempView("hbdata")
    val sumdata = spark.sql(
      """select srcdst,AVG(upbytesize) as mean_upbytesize from hbdata group by srcdst
      """.stripMargin)

    val llist = filterdomaindata.select("srcdst", "recordtime").distinct().rdd.map(line => (line.getAs[String]("srcdst"), Seq(line.getAs[String]("recordtime"))))
      .reduceByKey {
        (x, y) =>
          var ll: Seq[String] = Seq()
          ll = ll ++ x ++ y
          ll
      }
    //将时间长度大于3的过滤出来
    val lastdata: DataFrame = spark.createDataFrame(llist).toDF("srcdst", "timelist").distinct().filter(!_ (0).equals("")).filter(line => line.getAs[Seq[String]]("timelist").size > 3)
    val sumjoinlastdata = sumdata.join(lastdata,sumdata("srcdst")===lastdata("srcdst"),"inner").drop(lastdata("srcdst"))
    sumjoinlastdata.printSchema()
    //计算时间间隔列表
    val timeudf = udf((i: Seq[String]) => (timecompute(i)))
    val timedata = sumjoinlastdata.withColumn("timeintervalList", timeudf(sumjoinlastdata("timelist")))
    //计算循环自相关系数
    val autocorrelateudf = udf((seq: Seq[Double]) => (if (seq.isEmpty) 0.0 else maxautocorrelation(seq)))
    val autocorrelatedata = timedata.withColumn("autocorrelate", autocorrelateudf(timedata("timeintervalList")))

    val judgeudf = udf((seq: Double) => {
      if (seq == null) "0"
      else if (seq > 0.8) "1"
      else "0"
    })
    //过滤出有规律的时间间隔，即循环自相关系数大于0.8的
    val judgedata = autocorrelatedata.withColumn("judge", judgeudf(autocorrelatedata("autocorrelate"))).filter("judge == '1'").distinct()
    //关联data，得到经纬度
    val hdata = hbdata.select("srcdst","srcip","dstip","srcport","dstport","protocol","proto7", "slatitude", "slongitude", "scountry", "sprovince", "scity", "dcountry", "dprovince", "dcity", "dlatitude", "dlongitude")
    val joindata = hdata.join(judgedata, judgedata("srcdst") === hdata("srcdst"), "inner").drop(hdata("srcdst")).distinct()
    println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

    //生成溯源五元组
    val eventSourceUdf = udf(
      (srcip: String, dstip: String, srcport: String, dstport: String, protocol: String) => {
        srcip + "," + dstip + "," + srcport + "," + dstport + "," + protocol
      })
    val df = joindata.withColumn("eventSource", eventSourceUdf(joindata("srcip"), joindata("dstip"), joindata("srcport"), joindata("dstport"), joindata("protocol")))
    val eventdf = df.groupBy("srcdst").agg(concat_ws("#", collect_set("eventSource"))).withColumnRenamed("concat_ws(#, collect_set(eventSource))", "eventSource")
      .withColumnRenamed("srcdst", "srcdstip_new")

    val eventdfjoindf = eventdf.join(df, eventdf("srcdstip_new") === df("srcdst"), "left").drop(eventdf("srcdstip_new")).drop(df("eventSource"))

    val dropeventdf = eventdfjoindf.select("srcip", "dstip", "srcport", "dstport", "protocol", "proto7","mean_upbytesize", "srcdst", "timelist", "timeintervalList", "eventSource",
      "slatitude", "slongitude", "scountry", "sprovince", "scity", "dcountry", "dprovince", "dcity", "dlatitude", "dlongitude")
      .distinct().na.drop(Array("srcip", "dstip", "srcport", "dstport", "protocol", "proto7","mean_upbytesize", "srcdst", "timelist", "timeintervalList", "eventSource",
      "slatitude", "slongitude", "scountry", "sprovince", "scity", "dcountry", "dprovince", "dcity", "dlatitude", "dlongitude"))
    //生成证据字段
    val evidenceudf = udf((srcip: String, timeintervalList: Seq[Double]) => {
      val firsttime = first.toString.substring(0, 4) + "-" + first.toString.substring(4, 6) + "-" + first.toString.substring(6, 8) + "-" + first.toString.substring(8, 10) + ":" + first.toString.substring(10, 12)
      val lasttime = last.toString.substring(0, 4) + "-" + last.toString.substring(4, 6) + "-" + last.toString.substring(6, 8) + "-" + last.toString.substring(8, 10) + ":" + last.toString.substring(10, 12)
      ("资产" + srcip + "在" + firsttime + "-" + lasttime + "该时间段内连接了"
        + (timeintervalList.size + 1).toString + "次，同时连接时间间隔(单位:秒):[" + timeintervalList.mkString(",") + "]也具有规律性")
    })

    val evidencedata = dropeventdf.withColumn("evidence", evidenceudf(dropeventdf("srcip"), dropeventdf("timeintervalList")))

    //生成加密字段
    val tlsudf = udf((proto7: String) => (if (proto7 == "tls") "加密流量" else "非加密流量"))
    val tlsdata = evidencedata.withColumn("victimtype", tlsudf(evidencedata("proto7"))).drop(evidencedata("proto7")).distinct()

    //生成original_log字段
    //[共有字段:]  五元组  模型类型  发生时间  上报ip    [模型特有字段:]  连接间隔  连接次数
    val original_logudf = udf(
      (srcip: String, dstip: String, dstport: String, proto: String,
       timelist: Seq[String], timeintervalList: Seq[Double]) =>
        ("{\"源IP\":\"" + srcip + "\"," +
          "\"目的IP\":\"" + dstip + "\"," +
          "\"源端口\":\"" + "" + "\"," +
          "\"目的端口\":\"" + dstport + "\"," +
          "\"协议\":\"" + proto + "\"," +
          "\"模型类型\":\"" + "间歇会话连接" + "\"," +
          "\"发生时间\":\"" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timelist(0).toLong*1000) + "\"," +
          "\"上报IP\":\"" + InetAddress.getLocalHost().getHostAddress + "\"," +
          "\"连接间隔\":\"" + "[" + timeintervalList.mkString(",") + "]" + "\"," +
          "\"连接次数\":\"" + (timeintervalList.size + 1) + "\"}"))

    val original_logdata = tlsdata.withColumn("original_log", original_logudf(tlsdata("srcip"), tlsdata("dstip"), tlsdata("dstport"),
      tlsdata("protocol"), tlsdata("timelist"), tlsdata("timeintervalList"))).distinct()


    //匹配威胁情报和dga域名
    /** 都不匹配  0
      * 威胁情报 1
      * dga域名  2
      * 都匹配中  3
      */

    val threadUdomainSet = threaddata.distinct().rdd.map(x => x.toString()).collect().toSet
    val domaindataSet = domaindata.distinct().rdd.map(x => x.toString()).collect().toSet

    val judgeThreadDomainudf = udf((ip: String) => {
      val tdS = threadUdomainSet.contains(ip)
      val daS = domaindataSet.contains(ip)

      (tdS, daS) match {
        case (false, false) => "0"
        case (true, false) => "1"
        case (false, true) => "2"
        case (true, true) => "3"
      }
    })
    val judgeThreadAndDomian = original_logdata.withColumn("standby01", judgeThreadDomainudf(original_logdata("srcip")))

    import spark.implicits._
    val endf = judgeThreadAndDomian.select( $"srcip", $"dstip", $"dstport", $"protocol",$"mean_upbytesize", $"timelist", $"timeintervalList", $"eventSource", $"evidence", $"victimtype", $"original_log", $"standby01",
      $"slatitude", $"slongitude", $"scountry", $"sprovince", $"scity", $"dcountry", $"dprovince", $"dcity", $"dlatitude", $"dlongitude").distinct().map {
      case Row( srcip: String, dstip: String, dstport: String, protocol: String,mean_upbytesize:Double, timelist: Seq[String], timeintervalList: Seq[Double], eventSource: String, evidence: String, victimtype: String, original_log: String, standby01: String
      , slatitude: String, slongitude: String, scountry: String, sprovince: String, scity: String, dcountry: String, dprovince: String, dcity: String, dlatitude: String, dlongitude: String) =>
        val abnormaltype = first.toString.substring(0, 4) + "-" + first.toString.substring(4, 6) + "-" + first.toString.substring(6, 8) + "-" + first.toString.substring(8, 10) + ":" + first.toString.substring(10, 12)

        eventRes(genaralROW(), srcip, "间歇会话连接", new Date().getTime.toString.substring(0, 10).toLong * 1000, evidence, eventSource.split("#").take(10).mkString("#"), 0, srcip, dstip, dstport, (timeintervalList.size + 1).toString, abnormaltype, protocol, standby01, mean_upbytesize.toString, "",
          "MODEL_HEARTBEAT_MV1.0_001_0010", protocol, InetAddress.getLocalHost().getHostAddress, "AttackBasic6B10",
          scountry + "#" + sprovince + "#" + scity + "#" + slatitude + "#" + slongitude + "#" + dcountry + "#" + dprovince + "#" + dcity + "#" + dlatitude + "#" + dlongitude,
          original_log, victimtype, standby01,srcip,dstip,"",dstport)


    }.toDF("id","ip", "modeltype", "resulttime", "evidence", "eventsource", "happen", "normaltime", "abnormaltime", "abnormalport", "abnormalproto",
      "abnormaltype", "hisbaseline", "standby01", "standby02", "standby03", "event_rule_id", "proto", "reportneip", "event_sub_type", "position",
      "original_log", "encrypted", "event_flag","srcip","dstip","srcport","dstport")
    endf
  }

  //计算时间间隔
  def timecompute(list: Seq[String]): Seq[Double] = {
    var l: Seq[Double] = Nil
    if (list == null) {
      Nil
    } else {
      for (i <- 1 to (list.size - 1)) {
        val time = ((list(i).toLong - list(i - 1).toLong)).toDouble
        l = l :+ time
      }
    }
    l
  }

  //连加
  def chainplus(timestampseq: Seq[Double]) : Double = {
    var result = 0.0
    val timestampseq_r_0 = timestampseq.map {
      x => result += x * x
    }
    result
  }

  /**
    *
    * @param timelistseq
    * @return 间隔从1变化到k，分别计算 a(k),最后返回a(k)max  计算循环自相关系数
    */

  def maxautocorrelation(timelistseq: Seq[Double]): Double = {
    val r_0 = chainplus(timelistseq)
    var result: Seq[Double] = Nil
    for (j <- 1 until timelistseq.size) {
      var timelistseq_r_zj = 0.0
      var timelistseq_r = 0.0
      for (i <- 0 to math.ceil(timelistseq.size.toDouble / j).toInt - 1) {
        timelistseq_r = timelistseq_r + timelistseq_r_zj * timelistseq(i * j)
        timelistseq_r_zj = timelistseq(i * j)
      }
      result = result :+ (timelistseq_r / r_0)
    }
    //    println(result.isTraversableAgain)
    result.max
  }


  def genaralROW(): String = {
    var row = ((Math.random * 9 + 1) * 100000).toInt + "" //六位随机数
    row += new Date().getTime / 1000
    row
  }

  //创建es表名
  def getEStableIndex(): String = {
    val cal = Calendar.getInstance //实例化Calendar对象
    cal.add(Calendar.DATE, 0)
    val tablelong: String = new SimpleDateFormat("yyyyMMdd").format(cal.getTime) //设置格式并且对时间格式化
    tablelong
  }

  //取当前时间的小时数
  def getfirstTimeIndex(): Long = {
    val cal = Calendar.getInstance //实例化Calendar对象
    cal.add(Calendar.DATE, 0)
    val yearMonthDay: String = new SimpleDateFormat("yyyyMMdd").format(cal.getTime) //设置格式并且对时间格式化
    val hour: String = new SimpleDateFormat("HH").format(cal.getTime) //设置格式并且对时间格式化
    val l = (yearMonthDay + hour + "00").toLong
    l
  }

  //读取ES数据
  def getDataFromES(tablename: String, spark: SparkSession): DataFrame = {
    //    val query = "{\"query\":{\"range\":{\"recordtime\":{\"gte\":{\"lte\":}}}}}"
    val rawDF = spark.esDF(tablename)
    rawDF
  }

  //读取postgre数据
  def getDataFromPostgre(spark: SparkSession,jdbcurl:String,user:String,password:String,tablename:String):DataFrame ={
  //连接数据库

    val connectionProperties = new Properties()
    logger.error("连接数据库")
    try {
      connectionProperties.put("user", user)
      connectionProperties.put("password",password )
      connectionProperties.put("driver", "org.postgresql.Driver")
      logger.error("连接成功")
    } catch {
      case e: Exception => {
        logger.error(s"连接postgre数据库失败！！！" + e.getMessage)
      }
    }
    val data = spark.read.jdbc(jdbcurl,tablename,connectionProperties)
    data
  }

  }




case class eventRes(id:String,ip:String,modeltype: String = "间歇会话连接",resulttime:Long,evidence:String,eventsource:String,
                    happen:Int = 0 ,normaltime:String,abnormaltime:String,abnormalport:String,abnormalproto:String,
                    abnormaltype:String,hisbaseline:String,standby01:String,standby02:String,standby03:String,
                    event_rule_id:String,proto:String,reportneip:String,event_sub_type:String,position:String,
                    original_log:String,encrypted:String,event_flag:String,srcip:String,dstip:String,srcport:String,dstport:String)

