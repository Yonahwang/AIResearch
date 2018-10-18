package jobOneDay.subJob.Botnet.subClass

import java.io.{File, PrintWriter}
import java.sql.DriverManager
import java.util.{Date, Properties}

import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql._

/**
  * Created by wzd on 2018/1/17.
  */
trait saveMethod extends loggerSet{

  //保存模型结果表
  def saveESResult(data: DataFrame, properties: Properties) = {
    //添加时间
    val now: Long = new Date().getTime
    val addTime = org.apache.spark.sql.functions.udf(() => {now})
    val dataT = data.withColumn("time", addTime())
    //添加协议
    val addProto = org.apache.spark.sql.functions.udf(() => {"dns"})
    val dataTP = dataT.withColumn("proto", addProto())
    //添加事件类型
    val addType = org.apache.spark.sql.functions.udf(() => {"3"})
    val dataTPT = dataTP.withColumn("type", addType())
    //选出将要存储的字段
    val dataSave = dataTPT.select(dataTPT("id"),
      dataTPT("srcip").as("ip"),
      dataTPT("srcip"),
      dataTPT("proto"),
      dataTPT("type"),
      dataTPT("time"),
      dataTPT("trace").as("typeresult"),
      dataTPT("probability").as("judgmentresult"),
      dataTPT("subnet").as("calculategist"),
      dataTPT("botnet_num").as("calculateresult"),
      dataTPT("subnet_pc_num").as("encryptedtunnel"),
      dataTPT("subnet_botnet_probability").as("externalconnection")
    )
    saveToES(dataSave, "es.save.result", properties)
  }



  //保存到文件
  def saveToFile(data: DataFrame, path: String, fileName: String) = {
    //判断待写入文件是否存在，存在则删除
    val file = new File(path, fileName).exists()
    if (file) {
      new File(path, fileName).delete()
    }
    //写文件
    val column = data.columns
    //获取df列名
    val writer = new PrintWriter(new File(path, fileName))
    //创建文件读写对象
    val firstLine = column.mkString("\t") + "\n" //写入列名
    writer.write(firstLine)
    data.rdd.collect().foreach {
      //写入数据
      line =>
        val lineString = column.map { each => line.getAs[String](each) }.reduce { (x, y) => x + "\t" + y } + "\n" //将数据写到一个String
        writer.write(lineString)
    }
    writer.close()
    logger.error(s"write down $fileName!")
  }

  //保存到ES
  def saveToES(data: DataFrame, indexType: String, properties: Properties) = {
    val esoption = properties.getProperty(indexType)
    try {
      data.saveToEs(esoption)
      logger.error("complete:save to ES !")
    } catch {
      case e: Exception => logger.error("error on save to ES：" + e.getMessage)
    }
    finally {}
  }

  //保存到事件结果表
  def saveESEvent(data: DataFrame) = {
    //添加时间
    val now: Long = new Date().getTime
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

  }


  //保存到事件表
  def saveToPostgreSQLLine(data: DataFrame, properties: Properties) = {
    val table = properties.getProperty("postgre.table.name")
    logger.error("开始导入POSTGRESQL>>>")
    data.foreachPartition {
      part =>
        val conn_str = properties.getProperty("postgre.address")
        //        val conn_str = "jdbc:postgresql://172.16.1.108:5432/NXSOC5"
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              val sqlText =
                s"""INSERT INTO $table
                   |(event_rule_result_id, event_rule_name, reportneip,
                   |  sourceip, opentime, event_count,
                   |  event_type, event_base_type, event_sub_type,
                   |  srclatitude, srclongtitude, srcprovince,
                   |  srccountry, srccity, nodeid)
                   |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              //              prep.setTimestamp(2,new Timestamp(line.getAs[String]("date").toLong))
              prep.setString(1, line.getAs[String]("id"))
              prep.setString(2, "僵尸网络")
              prep.setString(3, line.getAs[String]("srcip"))
              prep.setString(4, line.getAs[String]("srcip"))
              prep.setString(5, line.getAs[String]("time"))
              prep.setString(6, "1")
              prep.setString(7, "异常流量事件")
              prep.setString(8, "6")
              prep.setString(9, "3") //b.srccountry, b.srcprovince, b.srccity, b.srclatitude, b.srclongtitude
              prep.setString(10, line.getAs[String]("srclatitude"))
              prep.setString(11, line.getAs[String]("srclongtitude"))
              prep.setString(12, line.getAs[String]("srcprovince"))
              prep.setString(13, line.getAs[String]("srccountry"))
              prep.setString(14, line.getAs[String]("srccity"))
              prep.setString(15, "0")
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
