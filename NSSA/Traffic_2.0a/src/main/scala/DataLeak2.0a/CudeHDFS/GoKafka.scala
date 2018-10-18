package SituationAwareness_2_0a.CudeHDFS

import java.net.InetAddress

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


class GoKafka extends SituationAwareness2.saveToKAFKA {
  val testing = false
  var cache_result = ArrayBuffer[(Array[String], Long, Int, Int)]()

  /**
    * 重新规划一下需要存的字段
    * 传入file_name，在这里统一进行切分？包含：srcip,dstip,srcport,dstport,file_type,result_time共6个
    * 在这里才进行计算的部分：id,record_time
    * 4个基本不变的属性：event_rule_id,report_ip,origin_log,flag
    *
    * 2018-7-27改动：新增字段，包括：filename原文件名，wordcloud慈词云统计
    */
  def cache_save(file_name: String, topics: Array[String],
                 brief: String, sensitivity_class: String,
                 result_type: Int, md5: String, flag: Int,
                 origin_file_name: String, word_cloud: String): Unit = {
    val example_filename = "1531308727000___4000_80_doc_ftp.data"

    //先解析file_name
    var anastr = if (testing) example_filename else if (file_name == "") "______.data" else file_name
    anastr = anastr.replaceAll("_", " _ ")
    val strs = anastr.substring(0, anastr.length - 5).split("_")

    val recordtime = if (strs(0).trim == "")
      System.currentTimeMillis()
    else
      strs(0).trim.toLong
    val srcip = strs(1).trim
    val dstip = strs(2).trim
    val srcport = strs(3).trim
    val dstport = strs(4).trim
    val file_type = strs(5).trim
    val protocol = strs(6).trim

    //然后设置topic
    val topic1 = if (topics.length >= 1) topics(0) else ""
    val topic2 = if (topics.length >= 2) topics(1) else ""
    val topic3 = if (topics.length >= 3) topics(2) else ""
    val topic4 = if (topics.length >= 4) topics(3) else ""
    //需要计算的部分
    val event_report_time = System.currentTimeMillis().toString
    val ran = new util.Random().nextInt(1000)
    var id = event_report_time
    if (ran.toString.length < 3)
      for (j <- 0 until 3 - ran.toString.length)
        id += "0"
    id += ran.toString
    //基本不会变的字段
    val event_rule_id = if (flag == 1) "MODEL_DATALEAK_MV1.0_001_0012"
    else "MODEL_FILEEXCHANGE_MV1.0_001_0012"
    val report_ip = InetAddress.getLocalHost.getHostAddress
    val origin_log =
      s"""{"id":"${id}",
         |"md5":"${md5}",
         |"srcip":"${srcip}",
         |"dstip":"${dstip}",
         |"srcport":"${srcport}",
         |"dstport":"${dstport}",
         |"file_type":"${file_type}",
         |"recordtime":${recordtime.toString},
         |"topic1":"${topic1}",
         |"topic2":"${topic2}",
         |"topic3":"${topic3}",
         |"topic4":"${topic4}",
         |"protocol":"${protocol}",
         |"abstarct":"${brief}",
         |"sensitivity_class":"${sensitivity_class}",
         |"event_report_time":${event_report_time.toString},
         |"event_rule_id":"${event_rule_id}",
         |"report_ip":"${report_ip}",
         |"flag":${flag.toString},
         |"result_type":${result_type.toString}}""".stripMargin
    //    println(origin_log)
    //    println("---------------------------------------------")
    val result_str = Array(id, md5, srcip, dstip, srcport, dstport, file_type,
      topic1, topic2, topic3, topic4, protocol, brief, sensitivity_class,
      event_report_time, event_rule_id, report_ip, origin_log, origin_file_name, word_cloud)
    //    cache_result += ((id, md5, srcip, dstip, srcport, dstport, file_type, recordtime,
    //      topic1, topic2, topic3, topic4, protocol, brief, sensitivity_class,
    //      event_report_time, event_rule_id, report_ip, origin_log, flag, result_type,
    //      origin_file_name, word_cloud))
    cache_result += ((result_str, recordtime, flag, result_type))
  }

  def trans_rdd(spark: SparkSession): Unit = {
    println(cache_result.length)
    val rdd = spark.sparkContext.parallelize(cache_result)
    import spark.implicits._
    //    val df = rdd.toDF("id", "md5", "srcip", "dstip", "srcport", "dstport",
    //      "file_type", "recordtime", "topic1", "topic2", "topic3", "topic4",
    //      "protocol", "abstract", "sensitivity_class", "event_report_time",
    //      "event_rule_id", "report_ip", "origin_log", "flag", "result_type",
    //      "filename", "wordcloud")
    val df = rdd.toDF("arr_attr", "recordtime", "flag", "result_type")
    val res_df = df.select(df.col("recordtime"),
      df.col("flag"),
      df.col("result_type"),
      df.col("arr_attr").getItem(0).as("id"),
      df.col("arr_attr").getItem(1).as("md5"),
      df.col("arr_attr").getItem(2).as("srcip"),
      df.col("arr_attr").getItem(3).as("dstip"),
      df.col("arr_attr").getItem(4).as("srcport"),
      df.col("arr_attr").getItem(5).as("dstport"),
      df.col("arr_attr").getItem(6).as("file_type"),
      df.col("arr_attr").getItem(7).as("topic1"),
      df.col("arr_attr").getItem(8).as("topic2"),
      df.col("arr_attr").getItem(9).as("topic3"),
      df.col("arr_attr").getItem(10).as("topic4"),
      df.col("arr_attr").getItem(11).as("protocol"),
      df.col("arr_attr").getItem(12).as("brief"),
      df.col("arr_attr").getItem(13).as("sensitivity_class"),
      df.col("arr_attr").getItem(14).as("event_report_time"),
      df.col("arr_attr").getItem(15).as("event_rule_id"),
      df.col("arr_attr").getItem(16).as("report_ip"),
      df.col("arr_attr").getItem(17).as("origin_log"),
      df.col("arr_attr").getItem(18).as("filename"),
      df.col("arr_attr").getItem(19).as("wordcloud"))
    //    id, md5, srcip, dstip, srcport, dstport, file_type,
    //    topic1, topic2, topic3, topic4, protocol, brief, sensitivity_class,
    //    event_report_time, event_rule_id, report_ip, origin_log, origin_file_name, word_cloud
    res_df.persist()
    //    res_df.filter("flag=2").show(10, false)
    //    res_df.printSchema()
    toKafka(spark, ("10.130.10.60:9092", "topic-dataleak"), res_df)
    res_df.unpersist()
  }
}
