package jobOneDay.subJob.CleanData.CleanDataClass

import java.io.Serializable
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import jobOneHour.subClass.LoggerSupport

/**
  * Created by TTyb on 2018/1/23.
  */
class CleanDataHDFS(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {
  def readyMain(): Unit = {
    if (properties.getProperty("deleteConf") == "true") {
      deleteTHostAttacks()
      deleteTModelResult()
    }
    cleanData()
  }

  //获取文件的名字
  def getFileName(filePath: String, index: String): Array[String] = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd0000")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    logger.error("删除" + filePath + ":" + yesterday + "之前的数据")

    //获取前缀后缀
    var prefix = ""
    var suffix = ""

    var arrayFile: Array[String] = new ArrayBuffer[String]().toArray
    try {
      val configuration = new Configuration()
      val output = new Path(filePath)
      val hdfs = output.getFileSystem(configuration)
      val fs = hdfs.listStatus(output)
      val fileName = FileUtil.stat2Paths(fs)
      hdfs.close()

      fileName.foreach { eachfile =>
        val eachFileName = eachfile.getName.split("\\.")
        prefix = eachFileName.head.replace(filePath, "").replace(index, "")
        suffix = eachFileName.last
        //prefix.toLong >= beforeYesterday.toLong && prefix.toLong < yesterday.toLong
        if (prefix.toLong < yesterday.toLong) {
          arrayFile = arrayFile :+ filePath + "/" + index + prefix.toLong.toString + "." + suffix
        }
      }
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    arrayFile
  }

  //获取文件名字
  def getFileNameArr(): Array[String] = {
    val hdfsUrl = properties.getProperty("hdfs.path")
    //原始数据的路径
    val NetFilePath: String = hdfsUrl + properties.getProperty("netflowHDFS")
    val HttpFilePath: String = hdfsUrl + properties.getProperty("httpHDFS")
    val DnsFilePath: String = hdfsUrl + properties.getProperty("DNSHDFS")
    val FtpFilePath: String = hdfsUrl + properties.getProperty("ftpHDFS")
    val SmtpFilePath: String = hdfsUrl + properties.getProperty("smtpHDFS")
    //预处理后数据的路径
    val NetPrepareFilePath: String = hdfsUrl + properties.getProperty("netPrepareHDFS")
    val HttpPrepareFilePath: String = hdfsUrl + properties.getProperty("httpPrepareHDFS")
    val DnsPrepareFilePath: String = hdfsUrl + properties.getProperty("DNSPrepareHDFS")

    val arrayFile = getFileName(NetFilePath, "netflow") ++ getFileName(HttpFilePath, "http") ++ getFileName(DnsFilePath, "dns") ++
      getFileName(FtpFilePath, "ftp") ++ getFileName(SmtpFilePath, "smtp") ++
      getFileName(NetPrepareFilePath, "") ++ getFileName(HttpPrepareFilePath, "") ++ getFileName(DnsPrepareFilePath, "")
    arrayFile
  }

  //删除数据
  def cleanData(): Unit = {
    val arrayFile = getFileNameArr()
    arrayFile.foreach { eachfile =>
      val configuration = new Configuration()
      val output = new Path(eachfile)
      val hdfs = output.getFileSystem(configuration)
      hdfs.delete(output, true)
      hdfs.close()
    }
    logger.error("delete:" + arrayFile.toList)
  }

  //删除模型结果表内数据
  def deleteTHostAttacks(): Unit = {
    try {
      val conn_str = properties.getProperty("postgre.address")
      Class.forName("org.postgresql.Driver").newInstance
      val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
      val table = properties.getProperty("thostattacks")
//      logger.error("delete:" + table)
      val sqlText = s"""DELETE FROM $table """
      val prep = conn.prepareStatement(sqlText)
      prep.executeUpdate
    } catch {
      case e: Exception => logger.error("删除原来的数据出错" + e.getMessage)
    }
  }

  def deleteTModelResult(): Unit = {
    try {
      val conn_str = properties.getProperty("postgre.address")
      Class.forName("org.postgresql.Driver").newInstance
      val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
      val table = properties.getProperty("tmodelresult")
//      logger.error("delete:" + table)
      val sqlText = s"""DELETE FROM $table """
      val prep = conn.prepareStatement(sqlText)
      prep.executeUpdate
    } catch {
      case e: Exception => logger.error("删除原来的数据出错" + e.getMessage)
    }
  }
}
