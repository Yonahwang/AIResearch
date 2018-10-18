package jobOneHour.subJob.dnsCovDetect

import java.net.InetAddress
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import jobOneHour.subClass.{LoggerSupport, saveToKAFKA}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{collect_set, concat_ws, udf}



case class kafR(id: String,ip: String,modeltype: String,resulttime: Long,evidence: String,eventsource: String,
                happen: Int,normaltime: String,abnormaltime: String,abnormalport: String,abnormalproto: String,
                abnormaltype: String,hisbaseline: String,standby01: String,standby02: String,standby03: String,
                event_rule_id: String,proto: String,reportneip: String,event_sub_type: String,position: String,
                original_log: String,encrypted: String,event_flag: String,srcip: String,dstip: String,srcport: String,dstport:String)




class SaveResult(predictions: DataFrame, spark: SparkSession, properties: Properties) extends Serializable with saveToKAFKA with LoggerSupport {

  import spark.implicits._

  //    提取异常的流量数据,全量,含地理位置信息
  val abnormal_df = predictions.filter($"prediction" === 1.0)
//  println("abnormal_df schema")
//  abnormal_df.printSchema()
//  abnormal_df.show()


  //    提取异常的流量数据，仅含特征字段
  val abnormal_df_f = abnormal_df.drop("flow_id","timestamp","dstip","srcport","dstport","protocol","slatitude","slongitude","sprovince","scountry","scity","dlatitude","dlongitude","dprovince","dcountry","dcity")
//  println("abnormal_df_f schema")
//  abnormal_df_f.printSchema()


  /**
    * 统计每个ip的异常请求和应答次数，请求字节数
    * 返回的dataframe每一行是一个ip的统计数据，包含请求次数request和应答次数response，请求传输的字节数，代表可能泄露隐私数据的规模（无应答字节数）
    * req统计一个df，res统计一个df，然后join
    * 忽略只有req或者resp异常的情况
    * 如果根据flags_response字段进行一次统计groupBy，没法把一个ip的两行数据拼接成两列
    */
  def counts_req_resp(df: DataFrame): DataFrame ={
    df.createOrReplaceTempView("df_f_view")
    val df_res = spark.sql("SELECT srcip, count(*) request,sum(size) size FROM df_f_view WHERE flags_response=0 GROUP BY srcip")
//    df_res.show()
    val df_req = spark.sql("SELECT srcip, count(*) response FROM df_f_view WHERE flags_response=1 GROUP BY srcip")
//    df_req.show()
    val df_req_resp = df_req.join(df_res, "srcip")
//    df_req_resp.show()
    df_req_resp
  }

  /**
    *  针对每一条流量，拼接ip地址和端口号作为溯源信息作为新的一列
    *  flow_id|srcip|timestamp|queries_name|flags_response|prediction|size|dstip|srcport|dstport|protocol|eventSource|
    */
  def event_source(df:DataFrame): DataFrame ={
    val event_source = df.withColumn("eventSource",concat_ws(",",df("srcip"),df("dstip"),
      df("srcport"),df("dstport"),df("protocol")))
//    println("ip source 溯源信息")
    event_source
  }

  /**
    * 拼接特征作为新的一列
    */
  def concate_features(df: DataFrame)={
    val features = df.withColumn("conc_features", concat_ws(",",df("queriesNameLen"),df("queriesLabelCal")
    ,df("stringEntropy"),df("numRatio"),df("sumAnswerLen"),df("proto"),df("flags_response"),df("flags_truncated")
      ,df("question_rrs"),df("answer_rrs"),df("authority_rrs"),df("additional_rrs"),df("queries_type"),df("maxAnswersTTL")
    ))
    features
  }

//

  /**
    * 聚合每一个ip的拼接溯源信息字段，拼接域名字段，拼接溯源信息字段 (用groupBy，去重)
    * 这里能否collect_set只连接十个域名，怎么设置？
    * @return
    */
  def aggregate(df: DataFrame):DataFrame ={
    val source_agg: DataFrame = df.select("queries_name","srcip","eventSource")
      .groupBy("srcip")
      .agg(concat_ws("#",collect_set("eventSource")) as "eventSourceAgg"
        ,concat_ws("#",collect_set("queries_name")) as "queriesNameAgg"
        //        ,concat_ws("#",)
      )
    //    println("ip agg(source) agg(name)")
    //    source_agg.show(false)
    source_agg
  }

  def aggregate_remove(df: DataFrame):DataFrame ={
    val source_agg: DataFrame = df.select("queries_name","srcip","eventSource","conc_features")
      .groupBy("srcip")
      .agg(concat_ws("#",collect_set("eventSource")) as "eventSourceAgg"
        ,concat_ws("#",collect_set("queries_name")) as "queriesNameAgg"
        ,concat_ws("#",collect_set("conc_features")) as "conc_featuresAgg"
//        ,concat_ws("#",)
      )
//    println("ip agg(source) agg(name)")
//    source_agg.show(false)
    source_agg
  }

  /**
    * 生成此刻时间,通常作为入库时间
    * @return
    */
  def getCurrentTime(): Long = {
    val nowTime: Long = new Date().getTime.toString.substring(0, 10).toLong * 1000
    nowTime
  }

  val currentTime_udf = udf(
    ()=>
      getCurrentTime()
  )

  /**
    * 根据时间戳计算日期 小时
    * @param t
    * @return
    */
  def timeZone(t: Int) ={
    val time = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(t.toLong * 1000))
    time + ":00"
  }

  val timeZone_udf = udf(
    (t:Int) =>
      timeZone(t)
  )

  /**
    * 求最大时间戳，根据时间戳计算发生时间段happenTimeZone
    * 发生时间段用于evidence字段
    */
  def happenTime(dataFrame: DataFrame) = {
    import spark.implicits._
    //    取一个ip的最大时间戳
    val ip_time = dataFrame.rdd.map{
      line=>
        //        type problem mark
        (line.getAs[String]("srcip"),line.getAs[String]("timestamp").toInt)
    }.keyBy(_._1)
      .reduceByKey{
        (ele1,ele2)=>
          (ele1._1,math.max(ele1._2,ele2._2))
      }.map(_._2).toDF("srcip","happenTime")

    val timeZone = ip_time.withColumn("happenTimeZone",timeZone_udf(ip_time("happenTime")))
    timeZone
  }

  /**
    * 生成唯一ID
    * @return
    */
  def generateID(): String = {
    val row1 = ((math.random * 9 + 1) * 100000).toInt.toString
    val row2 = (new Date().getTime / 1000).toString
    row1 + row2
  }

  val generateID_udf = udf(
    () =>
      generateID()
  )

  /**
    * 拼接happenTimeZone，ip，req，resp，size列生成evidence字段
    * @param time
    * @param ip
    * @param req
    * @param resp
    * @param size
    * @return
    */
  def addEvi(time: String, ip: String, req: String, resp: String, size: Long) ={
    val tStart = new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(time).getTime()
    val tEnd = tStart + 3600000
    val time2 = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(tEnd)
    val evidence =  "资产" + ip + "在" + time + "-" + time2 + " 异常DNS请求次数: " + req + " 异常DNS应答次数: " + resp + " 向外输出字节数: " + size
    evidence
  }

  val addEvi_udf = udf(
    (time: String, ip: String, req: String, resp: String, size: Long)=>
      addEvi(time, ip, req, resp, size)
  )


  def save(){
//    对每个ip，生成请求应答次数，请求字节数统计信息，生成src,request,response,size列
    val df_req_resp = counts_req_resp(abnormal_df)
//    df_req_resp.show()//1

//    对每条流量，聚合ip和port，生成溯源字段，增加eventSource列
//    val eventSource = event_source(abnormal_df).select("srcip","eventSource")
    val eventSource = event_source(abnormal_df)
//    eventSource.show()//

//     对每个ip，拼接溯源五元组，请求域名，对每个ip，仅生成src,eventSourceAgg，queriesNameAgg列
    val sta_source_agg = aggregate(eventSource)
//    sta_source_agg.show()//2

//    根据时间戳计算发生时间段,仅生成src,happenTimeZone列
    val agg_time = happenTime(abnormal_df)
//    agg_time.show()//

//    对每条流量，根据五列生成evidence字段，生成evidence列
//    全量*aggregation
    val time_req_resp = agg_time.join(df_req_resp,"srcip").join(sta_source_agg,"srcip").join(abnormal_df,"srcip").drop(abnormal_df("size"))
//    println("time_req_resp")
//    time_req_resp.show()
    val df_evi = time_req_resp.withColumn("evidence",
      addEvi_udf(time_req_resp("happenTimeZone"),time_req_resp("srcip"),time_req_resp("request"),time_req_resp("response"),time_req_resp("size")))
//    df_evi.show()//use1=>3

//    对每条流量，拼接特征字段，增加conc_features列
//    val featureAgg = concate_features(abnormal_df).select("srcip","conc_features")
    val featureAgg = concate_features(df_evi)
//    featureAgg.show()

//    对每条流量，生成结果时间resulttime列
    val df_evi_ID = featureAgg.withColumn("resulttime",currentTime_udf())
//    df_evi_ID.show()//4

    //生成唯一ID
      val df_kafka = df_evi_ID.rdd.map{
        line =>
          val id = ROWUtils.genaralROW()
          val ip = line.getAs[String]("srcip")  //目标地址
          val modeltype = "隐蔽通道"
          val resulttime = line.getAs[Long]("resulttime")  //存入发现异常时间（时间戳）
          val evidence = line.getAs[String]("evidence")
          val eventsource = line.getAs[String]("eventSourceAgg") //溯源信息
          val happen = 0
          val normaltime = line.getAs[String]("happenTimeZone") //异常发生时间段
        val abnormaltime = line.getAs[Long]("request").toString  //异常DNS请求次数
        val abnormalport = line.getAs[Long]("response").toString  //异常DNS应答次数
        val abnormalproto = line.getAs[Long]("queriesNameAgg").toString //异常域名拼接
        val abnormaltype = line.getAs[Long]("size").toString  //向外传输字节数
        val hisbaseline = line.getAs[Long]("conc_features").toString
        val standby01 = ""
        val standby02 = ""
        val standby03 = ""
        val event_rule_id = "MODEL_HIDECHANNEL_MV1.0_001_005"
        val proto = "DNS"
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_sub_type = "AttackBasic6B5"
        val srccountry = line.getAs[String]("scountry")
        val srcprovince = line.getAs[String]("sprovince")
        val srccity = line.getAs[String]("scity")
        val srclatitude = line.getAs[String]("slatitude")
        val srclongitude = line.getAs[String]("slongitude")
        val dstcountry = line.getAs[String]("dcountry")
        val dstprovince = line.getAs[String]("dprovince")
        val dstcity = line.getAs[String]("dcity")
        val dstlatitude = line.getAs[String]("dlatitude")
        val dstlongitude = line.getAs[String]("dlongitude")
        val position = Array(srccountry,srcprovince,srccity,srclatitude,srclongitude,dstcountry,dstprovince,dstcity,dstlatitude,dstlongitude).mkString("#")

        val original_log = "{\"网元IP\":\"" + ip + "\"," +
          "\"目的IP\":\"" + "" + "\"," +
          "\"源端口\":\"" + "" + "\"," +
          "\"目的端口\":\"" + "" + "\"," +
          "\"协议\":\"" + proto + "\"," +
          "\"模型类型\":\"" + modeltype + "\"," +
          "\"发生时间\":\"" + resulttime + "\"," +
          "\"上报IP\":\"" + reportneip + "\"," +
          "\"异常DNS传输字节数\":\"" + line.getAs[Int]("size") + "\"," +
          "\"异常DNS请求次数\":\"" + line.getAs[Int]("request") + "\"," +
          "\"异常DNS应答次数\":\"" + line.getAs[Int]("response") + "\"}"

        val encrypted = ""
        val event_flag = "0"
        val srcip = line.getAs[String]("srcip")
        val dstip = line.getAs[String]("dstip")
        val srcport = line.getAs[String]("srcport")
        val dstport = line.getAs[String]("dstport")
        kafR(id,ip,modeltype,resulttime,evidence,eventsource,happen,normaltime,
          abnormaltime,abnormalport,abnormalproto,abnormaltype,hisbaseline,
          standby01,standby02,standby03,event_rule_id,proto,reportneip,
          event_sub_type,position,original_log,encrypted,event_flag,
          srcip,dstip,srcport,dstport)
    }.toDF
        println("kafkaData show")
//        df_kafka.show()
        val nodes = properties.getProperty("kafka.nodes").split(",")
        val topics = properties.getProperty("kafka.topic2")
        nodes.map{
          line =>
            toKafka(spark,(line, topics), df_kafka)
        }


  }

}
