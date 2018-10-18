package jobFiveMinute.subJob.DataPrepare.DataPrepareClass

import java.io.Serializable
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import jobFiveMinute.subClass.LoggerSupport
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
  * Created by TTyb on 2018/1/9.
  */
class DataPrepareHDFS(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {
  def readyMain(filePath: String, index: String, filename:String) {
    val startTime = new Date().getTime

    var elements = new ArrayBuffer[String]().toArray
    var savePath = ""
    if (filePath.contains("netflow")) {
      elements = properties.getProperty("netflowElements").split(",")
      savePath = properties.getProperty("hdfs.path") + "/" + properties.getProperty("saveNetflow")
    } else if (filePath.contains("http")) {
      elements = properties.getProperty("httpElements").split(",")
      savePath = properties.getProperty("hdfs.path") + "/" + properties.getProperty("saveHttp")
    } else if (filePath.contains("dns")) {
      elements = properties.getProperty("DNSElements").split(",")
      savePath = properties.getProperty("hdfs.path") + "/" + properties.getProperty("saveDns")
    }

    val IPS = getIpSet()
//    val fileName = getFileName(filePath, index)
    val fileName = "/" + filename.split("/").last.replace("netflow", index)

    val dataFrame = initData(getData(filePath, fileName), IPS, filePath, elements)
    saveData(dataFrame, fileName, savePath, index)

    val endTime = new Date().getTime
    logger_dp.error(filePath + "程序运行时间：" + (endTime / 1000 - startTime / 1000).toString + "秒")
  }

  // 读取数据出来
  def getData(filePath: String, fileName: String): DataFrame = {
    logger_dp.error("获取原始数据" + filePath)
    val hdfsUrl = properties.getProperty("hdfs.path") + "/"
    val dataPath = hdfsUrl + filePath + fileName
    val dataDF = spark.read.json(dataPath)
    dataDF
  }

  //格式化数据
  def initData(dataDF: DataFrame, IPS: List[String], filePath: String, elements: Array[String]): DataFrame = {
    /**
      * 筛选IP
      * flagIP = 0 不包含网元IP
      * flagIP = 1 网元IP在源IP
      * flagIP = 2 网元IP在目的IP
      * flagIP = 3 网元IP在源IP且在目的IP
      **/
    val IPUdf = org.apache.spark.sql.functions.udf(
      (srcip: String, dstip: String) => {
        var flagIP = 0
        if (IPS.contains(srcip)) {
          flagIP = 1
        } else if (IPS.contains(dstip)) {
          flagIP = 2
        } else if (IPS.contains(srcip) && IPS.contains(dstip)) {
          flagIP = 3
        }
        flagIP
      }
    )

    val dataDFN = dataDF.select(elements.head, elements.tail: _*).withColumn("flagIP", IPUdf(dataDF("srcip"), dataDF("dstip"))).where("flagIP <> 0")
    dataDFN
  }

  //获取倒数第二个文件的名字
  def getFileName(filePath: String, index: String): String = {
    var fileName = ""
    //获取前缀后缀
    var prefix = ""
    var suffix = ""

    var arrayFile: List[Long] = List(111111)
    val hdfsUrl = properties.getProperty("hdfs.path")
    val hadfPath = hdfsUrl + "/" + filePath

    val output = new Path(hadfPath)
    val Configuration = new Configuration()
    Configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    val hdfs = FileSystem.get(new java.net.URI(hdfsUrl), Configuration)
    val fs = hdfs.listStatus(output)
    val filepath = FileUtil.stat2Paths(fs)
    hdfs.close()
    filepath.foreach { eachfile =>
      val eachFileName = eachfile.getName.split("\\.")

      prefix = eachFileName.head.replace(hadfPath, "").replace(index, "")
      suffix = eachFileName.last
      arrayFile = arrayFile :+ prefix.toLong
    }

    fileName = "/" + index + arrayFile.sorted.init.last.toString + "." + suffix
    fileName
  }

  //将处理后的数据保存下来
  def saveData(dataFrame: DataFrame, fileName: String, savePath: String, index: String): Unit = {
    logger_dp.error("保存预处理结果" + savePath)
    val saveData = savePath + fileName.replace(index, "")
    saveAsJSON(saveData, dataFrame)
  }

  //获取ip列表
  def getIpSet(): List[String] = {
    logger_dp.error("获取网元ip")
    val deviceIPTable = properties.getProperty("asset.table")
    val query = s"(SELECT device_ip FROM $deviceIPTable where status<>2)"
    val driver = s"org.postgresql.Driver"
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    Class.forName(driver)
    val posgreConn: Connection = DriverManager.getConnection(address, username, password)
    val st: Statement = posgreConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
    val rs: ResultSet = st.executeQuery(query)
    var ipSet = List("")
    ipSet = ipSet.drop(1)
    while (rs.next()) {
      val ip = rs.getString(1)
      ipSet = ip :: ipSet
    }
    ipSet.distinct
  }

  //保存为一个json
  def saveAsJSON(savePath: String, data: DataFrame) {
    if (data.take(10).length != 0) {
      logger_dp.error(savePath)
      val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> savePath)
      data.repartition(1).write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(options).save()
    }
  }

}
