package jobOneDay.subJob.BotnetDetectBaseOnDNS.SubClass

import java.io.{File, PrintWriter, Serializable}
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._

/**
  * Created by wzd on 2018/1/17.
  */
trait saveMethod extends loggerSet with Serializable{


  //保存到文件
  def saveToFile(data: DataFrame, path: String, fileName: String) = {

    //("id","ip","recordtime","status","result_type","label","domain_name","session_time","session_count","botnet_probability","content","content_count","content_confusion","field01","field02","field03")
    val insertData = data.select(data("id").cast("String"),data("ip").cast("String"),data("recordtime").cast("String"),
      data("status").cast("String"),data("result_type").cast("String"),data("label").cast("String"),
      data("domain_name").cast("String"),data("session_time").cast("String"),data("session_count").cast("String"),
      data("botnet_probability").cast("String"),data("content").cast("String"),data("content_count").cast("String"),
      data("content_confusion").cast("String"),data("field01").cast("String"),data("field02").cast("String"),
      data("field03").cast("String"))

    //判断待写入文件是否存在，存在则删除
    val file = new File(path, fileName).exists()
    if (file) {
      new File(path, fileName).delete()
    }
    //写文件
    val column = insertData.columns
    //获取df列名
    val writer = new PrintWriter(new File(path, fileName))
    //创建文件读写对象
    val firstLine = column.mkString("\t") + "\n" //写入列名
    writer.write(firstLine)
    insertData.rdd.collect().foreach {
      //写入数据
      line =>
        val lineString = column.map { each => line.getAs[String](each) }.reduce { (x, y) => x + "\t" + y } + "\n" //将数据写到一个String
        writer.write(lineString)
    }
    writer.close()
    logger.error(s"write down $fileName!")
  }

  //保存到ES
  def saveToES(data: DataFrame, properties: Properties) = {
    val esoption = properties.getProperty("es.botnet.result")
    try {
      data.saveToEs(esoption)
      logger.error("complete:save to ES !")
    } catch {
      case e: Exception => logger.error("error on save to ES：" + e.getMessage)
    }
    finally {}
  }



  //保存到事件表
  def saveToPostgreSQLLine(data: DataFrame, properties: Properties, spark:SparkSession) = {
    val table_b = spark.sparkContext.broadcast(properties.getProperty("postgre.botnet.table"))
    val conn_str_b = spark.sparkContext.broadcast(properties.getProperty("postgre.address"))
    val user_b = spark.sparkContext.broadcast(properties.getProperty("postgre.user"))
    val password_b = spark.sparkContext.broadcast(properties.getProperty("postgre.password"))

    logger.error("开始导入POSTGRESQL>>>")
    data.rdd.foreachPartition {
      part =>
        val table = table_b.value
        val conn_str = conn_str_b.value
        val user = user_b.value
        val password = password_b.value
//        val conn_str = properties.getProperty("postgre.address")
        //        val conn_str = "jdbc:postgresql://172.16.1.108:5432/NXSOC5"
        Class.forName("org.postgresql.Driver").newInstance
//        val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
        val conn = DriverManager.getConnection(conn_str, user, password)
        part.foreach {
          line =>
            try {
              val sqlText =
                s"""INSERT INTO $table
                   |(id, ip, recordtime,
                   |  status, result_type, label,
                   |  domain_name, session_time, session_count,
                   |  botnet_probability, content, content_count,
                   |  content_confusion, field01, field02,
                   |  field03)
                   |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              //              prep.setTimestamp(2,new Timestamp(line.getAs[String]("date")
              prep.setString(1, line.getAs[String]("id"))
              prep.setString(2, line.getAs[String]("ip"))
              prep.setTimestamp(3, Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(line.getAs[String]("recordtime"))))
              prep.setString(4, line.getAs[String]("status"))
              prep.setString(5, line.getAs[String]("result_type"))
              prep.setInt(6, line.getAs[Int]("label"))
              prep.setString(7, line.getAs[String]("domain_name"))
              prep.setString(8, line.getAs[String]("session_time"))
              prep.setString(9, line.getAs[String]("session_count"))
              prep.setString(10, line.getAs[String]("botnet_probability"))
              prep.setString(11, line.getAs[String]("content"))
              prep.setString(12, line.getAs[String]("content_count"))
              prep.setString(13, line.getAs[String]("content_confusion"))
              prep.setString(14, line.getAs[String]("field01"))
              prep.setString(15, line.getAs[String]("field02"))
              prep.setString(16, line.getAs[String]("field03"))
              prep.executeUpdate
            } catch {
              case e: Exception => logger.error("导入出错" + e.getMessage)
            }
            finally {}
        }
        conn.close

    }
  }



}




//保存到事件结果表
//  def saveESEvent(data: DataFrame) = {
//    //添加时间
//    val now: Long = new Date().getTime
//添加信息
//    val result = data.map{
//      line =>
//        val id = line.getAs[String]("id")
//        val reportneip = line.getAs[String]("srcip")
//        val srcip = line.getAs[String]("srcip")
//        val event_rule_name = "僵尸网络"
//        val opentime = now
//        val event_count = "1"
//        val event_type = "2"
//        val event_base_type = "6"
//        val event_sub_type = "3"
//        val srclatitude = line.getAs[String]("srclatitude")
//        val srclongtitude = line.getAs[String]("srclongtitude")
//        val srcprovince = line.getAs[String]("srcprovince")
//        val srccountry = line.getAs[String]("srccountry")
//        val srccity = line.getAs[String]("srccity")
//        val nodeid = "0"
//        val event_rule_id = "0"
//        val logid = "0"
//        val event_rule_level = "0"
//        val event_detail = "0"
//        val actionopt = "0"
//        val is_inner = "0"
//        val reach = "0"
//        val url = "0"
//        val appproto = "0"
//        val getparameter = "0"
//        val infoid = "0"
//        val affectedsystem = "0"
//        val attackmethod = "0"
//        val appid = "0"
//        val victimtype = "0"
//        val attackflag = "0"
//        val attacker = "0"
//        val victim = "0"
//        val host = "0"
//        val filemd5 = "0"
//        val filedir = "0"
//        val referer = "0"
//        val cve = "0"
//        val requestmethod = "0"
//        val attacktype = "0"
//
//        (id, event_rule_name, reportneip,
//          srcip, opentime, event_count,
//          event_type, event_base_type, event_sub_type,
//          srclatitude, srclongtitude, srcprovince,
//          srccountry, srccity, nodeid,
//          event_rule_id,logid,event_rule_level,
//          event_detail,actionopt,is_inner,
//          reach,url,appproto,
//          getparameter,infoid,affectedsystem,
//          attackmethod,appid,victimtype,
//          attackflag,attacker,victim,
//          host,filemd5,filedir,
//          referer,cve,requestmethod,
//          attacktype)
//    }.toDF("event_rule_result_id","event_rule_name","reportneip",
//      "sourceip","opentime","event_count",
//      "event_type","event_base_type","event_sub_type",
//      "srclatitude","srclongtitude","srcprovince",
//      "srccountry","srccity","nodeid",
//      "event_rule_id","logid","event_rule_level",
//      "event_detail","actionopt","is_inner",
//      "reach","url","appproto",
//      "getparameter","infoid","affectedsystem",
//      "attackmethod","appid","victimtype",
//      "attackflag","attacker","victim",
//      "host","filemd5","filedir",
//      "referer","cve","requestmethod",
//      "attacktype")
//
//    //生成es索引
//    val time = new Date().getTime
//    val newtime = new SimpleDateFormat("yyyyMMdd").format(time)
//    val indexType = s"event_$newtime/event_$newtime"
//    //保存到ES
//    try {
//      result.saveToEs(indexType)
//      logger.error("complete:save to ES !")
//    }
//    catch {
//      case e: Exception => logger.error("error on save to ES：" + e.getMessage)
//    }

//}


