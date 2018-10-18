package jobOneHour.subJob.detectDGA.classmethod

import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by Administrator on 2018年1月15日 0015.
  */
trait commonFunc extends writeToLogger with Serializable {

  def readCsv(sparkSession: SparkSession, path: String): DataFrame = {
    val options = Map("header" -> "true", "path" -> path, "delimiter" -> ",")
    var csvDF: DataFrame = null
    try {
      csvDF = sparkSession.read
        .options(options)
        .format("com.databricks.spark.csv")
        .load()
      logger.error("Sucess read:" + path)
    } catch {
      case e: Exception => logger.error("Error read:" + e.getMessage)
    } finally {}
    csvDF
  }

  def readCsv1(sparkSession: SparkSession, path: String): DataFrame = {
    val options = Map("header" -> "true", "path" -> path, "delimiter" -> "\t")
    var csvDF: DataFrame = null
    try {
      csvDF = sparkSession.read
        .options(options)
        .format("com.databricks.spark.csv")
        .load()
      logger.error("Sucess read:" + path)
    } catch {
      case e: Exception => logger.error("Error read:" + e.getMessage)
        csvDF = null
    } finally {}
    csvDF
  }

  //  3.Reading text-File, saved Array
  def readText(sparkSession: SparkSession, path: String): Array[String] = {
    val sparkContext: SparkContext = sparkSession.sparkContext
    var textFile: Array[String] = null
    try {
      textFile = sparkContext.textFile(path).collect()
      logger.error("Sucess read:" + path)
    } catch {
      case e: Exception => logger.error("Error read:" + e.getMessage)
    } finally {}
    textFile
  }


  //  4.Saving Csv-File
  def saveCsv(dataframe: DataFrame, savePath: String) = {
    val saveOptions = Map("header" -> "true", "delimiter" -> ",", "path" -> savePath)
    dataframe.repartition(1).write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.Append)
      .options(saveOptions)
      .save()
    logger.error("数据保存成功，路径为：" + savePath)
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


  /**
    * 随机生成id标识
    *
    * @return
    */
  def generateID(): String = {
    val row1 = ((math.random * 9 + 1) * 100000).toInt.toString
    val row2 = (new Date().getTime / 1000).toString
    row1 + row2
  }

  /**
    * 生成当前时间
    * 输出格式为Long
    *
    * @return
    */
  def getNowTime(): Long = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      .parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date))
      .getTime
    date
  }

/*
  def getLongTime(timeH: String) = {
    val tf = new SimpleDateFormat("yyyyMMddHH")
    val date = tf.parse(timeH).getTime
    date
  }
*/

  /**
    * 格式为2018-06-14 10:55:40
    * @param timeH
    * @return
    */
  def getDateTime(timeH: String) = {
    val year = timeH.slice(0, 4)
    val month = timeH.slice(4, 6)
    val day = timeH.slice(6, 8)
    val hour = timeH.slice(8, 10)
    year + "-" + month + "-" + day + " " + hour + ":00:00"
  }

}

