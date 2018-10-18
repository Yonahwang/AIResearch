package jobFiveMinute.subJob.netflowProcess.classmethod

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Yiutto on 2018年1月15日 0015.
  */
trait corrFunc extends writeToLogger{

  /**
    * Creating sql.SparkSession
    *
    * @param properties
    * @return
    */
  def createSparkSession(properties: Properties) = {
    val masterSet = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val maxRetries = properties.getProperty("spark.port.maxRetries")
    val core = properties.getProperty("spark.cores.max")
    val memory = properties.getProperty("spark.executor.memory")
    val partition = properties.getProperty("spark.sql.shuffle.partitions")
    val sparkconf = new SparkConf()
      .setMaster(masterSet)
      .setAppName(appName)
      .set("spark.port.maxRetries", maxRetries)
      .set("spark.cores.max", core)
      .set("spark.executor.memory", memory)
      .set("spark.sql.shuffle.partitions", partition)
    val mySparkSession = SparkSession.builder().config(sparkconf).getOrCreate()
    mySparkSession
  }

  /**
    * 读取本地的csv文件
    *
    * @param sparkSession
    * @param path
    * @return
    */
  def readCsv(sparkSession: SparkSession, path: String): DataFrame = {
    val options = Map("header" -> "true", "path" -> path, "delimiter" -> "\t")
    var csvDF: DataFrame = null
    try {
      csvDF = sparkSession.read
        .options(options)
        .format("com.databricks.spark.csv")
        .load()
      logger_np.error("Success read csv-file:" + path)
    } catch {
      case e: Exception => logger_np.error("Error read csv-file:" + e.getMessage)
    } finally {}
    csvDF
  }

  /**
    * 读取postgrel的网元ip
    * @param properties
    * @return
    */
  def getIpSet(properties: Properties): List[String] = {
    val deviceIPTable = properties.getProperty("asset.table")
    val query = s"(SELECT device_ip FROM $deviceIPTable where status<>2)"
    val driver = s"org.postgresql.Driver"
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    println("获取网元ip" + address + ">>>" + deviceIPTable )
    Class.forName(driver)
    var ipSet = List("")
    val posgreConn: Connection = DriverManager.getConnection(address, username, password)
    try {
      val st: Statement = posgreConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val rs: ResultSet = st.executeQuery(query)

      ipSet = ipSet.drop(1)
      while (rs.next()) {
        val ip = rs.getString(1)
        ipSet = ip :: ipSet
      }
    } catch {
      case e: Exception => logger_np.error("Error read:" + e.getMessage)
    } finally {
      posgreConn.close()
    }

    ipSet.distinct
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

  def getNowTime(): Long = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      .parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date))
      .getTime
    date
  }
}

