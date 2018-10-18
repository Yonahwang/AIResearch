package jobOneHour.subJob.trafficAnomaly.classmethod

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Administrator on 2018年6月14日 0014.
  */
trait corrFuction extends writeToLogger{


  /**
    * 读取本地csv文件
    *
    * @param sparkSession
    * @param path
    * @return
    */
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
    } finally {
    }
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
    logger.error("获取网元ip"+ address + ">>>>" + deviceIPTable)
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
      case e: Exception => println("Error read:" + e.getMessage)
    } finally {
      posgreConn.close()
    }

    ipSet.distinct
  }

  def getlasttimeH(): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHH")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.HOUR, -1)
    val previous_hour = dateFormat.format(cal.getTime)

    previous_hour
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

 /* def getLongTime(timeH: String) = {
    val tf = new SimpleDateFormat("yyyyMMddHH")
    val date = tf.parse(timeH).getTime
    date
  }*/

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
  /**
    * 读取当前时刻在Postgrel的记录
    *
    * @param traffic_time
    * @return
    */
  def readPostgresqlData(traffic_time: String, mysparksession: SparkSession, properties: Properties, protocol: String): DataFrame = {
    val postgresqlUsr = properties.getProperty("postgre.user")
    val postgresqlPwd = properties.getProperty("postgre.password")
    val postgresqlUrl = properties.getProperty("postgre.address")
    val postgresqlTable = properties.getProperty("postgre.table.name.train")
    logger.info("读取Postgresql中table表" + postgresqlUrl + "-----" + postgresqlTable + "的信息, waiting.........")

    // 当前时刻的实际流量（原则上只有1条）
    val dbtableSql =
      s"""
         |(SELECT temp.standby02 AS ips, temp.baselineresult AS all_data FROM $postgresqlTable AS temp
         | WHERE temp.standby03 = '$protocol' AND temp.typeresult='$traffic_time'
         | AND temp.identityresult='ip_data' AND temp.type='异常流量'
         |) AS trafficdata""".stripMargin

    val options = Map("driver" -> "org.postgresql.Driver", "url" -> postgresqlUrl, "dbtable" -> dbtableSql,
      "user" -> postgresqlUsr, "password" -> postgresqlPwd)

    var tableDF: DataFrame = null

    try {
      tableDF = mysparksession.read
        .format("jdbc")
        .options(options)
        .load()
      if (tableDF.count() > 0) {
        println("Sucess read:" + postgresqlTable + traffic_time + ",获取当前数据")
      } else {
        println("Error read:" + postgresqlTable + traffic_time + "不存在此条记录")
      }
    } catch {
      case e: Exception => println("Error read:" + e.getMessage)
    } finally {
    }
    tableDF
  }
}
