package jobOneDay.subJob.RecoverNetflowData

import java.sql.DriverManager
import java.util.Properties

import jobOneDay.subJob.RecoverNetflowData.Logger4j
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Range
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018年6月28日 0028.
  */
class recoverNetflowMain(mysparksession: SparkSession, properties: Properties) extends Logger4j with Serializable{

  def main(): Unit = {
    // target的hdfs路径
    val target_netflowPath = properties.getProperty("target.hdfs.path")
    // 保存当前处理文件的文件名
    val valPath = properties.getProperty("target.save.path")
    // 读取之前保存的文件名
    var previous_ntfilename: String = readVarfromLocal(valPath)
    val filenameArr: Array[String] = getHDFSFileName(target_netflowPath)
    val fileArr_len = filenameArr.length

    try {
      if (fileArr_len > 0) {
        // 指定文件（取排序后的倒数第1个文件，最后那个文件可能没有完成）
        val current_ntfilename = filenameArr(fileArr_len - 1)
        if (current_ntfilename != previous_ntfilename) {
          // 获取hdfs文件路径
          val filepath = target_netflowPath + "/" + current_ntfilename
          val protocolArr = Array("tcp", "udp", "icmp")
          // 得到某个协议的dataframe
          for (protocol <- protocolArr) {
            // 得到当前协议需要恢复的数据
            val targetRDD: RDD[(String, String, String)] = getTargetNetflow(filepath, protocol)
            if (targetRDD != null) {
              updateAnomalyIpPostgresql(targetRDD, protocol)
            } else {
              logger.error("当前协议：" + protocol + "不存在误删的数据")
            }
          }

          // 保存当前文件名到本地
          writeVartoLocal(current_ntfilename, valPath)
        } else {
          logger.error("该文件已经处理过了，不需要处理")
        }
      } else {
        logger.error("HDFS目录下没有数据文件，不作处理")
      }
    } catch {
      case e: Exception => logger.error("Error：" + e.getMessage)
    } finally {
    }

  }

  def readPostgresqlData(traffic_time: String, protocol: String): (String, String) = {
    val postgresqlUsr = properties.getProperty("postgre.user")
    val postgresqlPwd = properties.getProperty("postgre.password")
    val postgresqlUrl = properties.getProperty("postgre.address")
    val postgresqlTable = properties.getProperty("postgre.table.name.train")
    logger.info("读取Postgresql中table表" + postgresqlUrl + "-----" + postgresqlTable + "的信息, waiting.........")

    // 当前时刻的实际流量（原则上只有1条）
    val dbtableSql =
      s"""
         |(SELECT temp.standby02 AS ips, temp.baselineresult AS all_data FROM $postgresqlTable AS temp
         | WHERE temp.typeresult='$traffic_time' AND temp.identityresult='ip_data' AND temp.type='异常流量'
         | AND temp.standby03='$protocol'
         |) AS trafficdata""".stripMargin

    val options = Map("driver" -> "org.postgresql.Driver", "url" -> postgresqlUrl, "dbtable" -> dbtableSql,
      "user" -> postgresqlUsr, "password" -> postgresqlPwd)

    var corr_data= ("", "")

    try {
      import mysparksession.implicits._
      val tableDF = mysparksession.read
        .format("jdbc")
        .options(options)
        .load()
      if (tableDF.count() > 0) {
        println("Sucess read:" + postgresqlTable + traffic_time + ",获取当前数据")
        corr_data = tableDF.map { line =>
          val ips = line.getAs[String]("ips")
          val ip_datas = line.getAs[String]("all_data")
          (ips, ip_datas)
        }.take(1)(0)
      } else {
        println("Error read:" + postgresqlTable + traffic_time + "不存在此条记录")
      }
    } catch {
      case e: Exception => println("Error read:" + e.getMessage)
    } finally {
    }
    corr_data
  }

  /**
    * 读取本地的文件内容，保存为String（读取上次处理过netflow的文件名，避免重复处理同一条数据）
    *
    * @param filePath
    * @return
    */
  def readVarfromLocal(filePath: String): String = {
    import scala.io.Source
    // 将文件的内容保存为字符串数组
    var str = ""
    val strArr = Source.fromFile(filePath).getLines().toArray
    if (strArr.length > 0) {
      str = strArr(0)
    }
    str
  }

  def getHDFSFileName(hdfsPath: String): Array[String] = {
    import java.net.URI

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(hdfsPath), conf)

    var nameArr = Array[String]()
    try {
      val fs = hdfs.listStatus(new Path(hdfsPath))
      nameArr = FileUtil.stat2Paths(fs).map(_.getName).sorted
    } catch {
      case e: Exception => logger.error("出错：" + e.getMessage)
    } finally {
      hdfs.close()
    }

    nameArr
  }

  def getTargetNetflow(filepath: String, protocol: String) = {
    var dataRDD: RDD[(String, String, String)] = null
    val targetDF = mysparksession.read.json(filepath)
      .where(s"""modeltype='异常流量' AND target='false'AND standby03='$protocol'""")
      .select("ip", "standby01", "standby02")
    if (targetDF.count() > 0) {
      var recoverRDD: RDD[(String, String)] = null
      val recoverArr = new ArrayBuffer[(String, String)]()
      targetDF.collect().foreach { line =>
        val ip = line.getAs[String]("ip")
        val anomaly_timeArr: Array[String] = line.getAs[String]("standby01").split("#")
        val anomaly_valueArr: Array[String] = line.getAs[String]("standby02").split("#")
        val len = anomaly_timeArr.length
        for (i <- Range(0, len)) {
          recoverArr += Tuple2(anomaly_timeArr(i), ip + "," + anomaly_valueArr(i))
        }
      }
      recoverRDD = mysparksession.sparkContext.makeRDD(recoverArr.toArray)
      import mysparksession.implicits._
      val recoverDF = recoverRDD.toDF("happentime", "ip_value").groupBy("happentime")
        .agg(concat_ws("#", collect_set("ip_value")))
        .withColumnRenamed("concat_ws(#, collect_set(ip_value))", "ips_values")
      // targetDF: happentime|ips_values
      dataRDD = recoverDF.rdd.map { line =>
        val happentime = line.getAs[String]("happentime")
        val recover_ips_values = line.getAs[String]("ips_values")
        val (ips, all_data) = readPostgresqlData(happentime, protocol)

        val new_all_data = replaceAnomalyIpValue(recover_ips_values, ips, all_data)

        (happentime, ips, new_all_data)
      }
    }
    dataRDD
  }

  def replaceAnomalyIpValue(recover_ips_values: String, ips: String, all_data: String): String = {
    val ipsArr: Array[String] = ips.split("#")
    val recover_ips_valuesArr: Array[String] = recover_ips_values.split("#")
    val new_all_dataArr: Array[String] = all_data.split("#").clone()

    if ((ipsArr.size > 0) && (ipsArr.size == new_all_dataArr.size)){
      recover_ips_valuesArr.foreach { recover_ip_value =>
        val ip_value: Array[String] = recover_ip_value.split(",")
        val index: Int = ipsArr.indexOf(ip_value(0))
        // 如果index=-1，则说明这个数组不存在这个值
        if (index > 0) {
          // 将异常ip对应的value值替换为NaN
          new_all_dataArr.update(index, ip_value(1))
        }
      }
    }

    new_all_dataArr.mkString("#")
  }

  /** *
    * 更新异常ip的流量值，入库更新
    *
    * @param edit_hour_current_trafficDF
    * @param protocol
    */
  def updateAnomalyIpPostgresql(edit_hour_current_trafficDF: RDD[(String, String, String)], protocol: String) = {
    // (happentime, ips, new_all_data)
    // 设置postgresql配置参数
    val table = properties.getProperty("postgre.table.name.train")
    val conn_url = properties.getProperty("postgre.address")

    logger.error("异常ip对应的值更新替换，准备更新导入" + conn_url + "=>" + table)
    edit_hour_current_trafficDF.foreachPartition {
      part =>
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_url, properties.getProperty("postgre.user"),
          properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              val (happentime, ips, new_all_data) = line
              val sqlText =
                s"""UPDATE $table SET baselineresult= '$new_all_data'
                   | WHERE typeresult='$happentime' AND type='异常流量' AND identityresult='ip_data'
                   | AND standby03='$protocol' AND standby02='$ips'
             """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              prep.executeUpdate
              logger.error("Successed: " + happentime + "->" + protocol + " --此条记录更新成功")
            } catch {
              case e: Exception => logger.error("更新出错" + e.getMessage)
            } finally {}

        }
        conn.close
    }
  }

  /**
    * 将String写入本地文件（将当前处理的netflow文件名保存至本地）
    *
    * @param variable
    * @param filePath
    */
  def writeVartoLocal(variable: String, filePath: String) = {
    import java.io._
    val writer = new PrintWriter(new File(filePath))
    writer.write(variable)
    logger.error("Successed: 变量值为" + variable + " SaveTo: " + filePath)
    writer.close()
  }


}
