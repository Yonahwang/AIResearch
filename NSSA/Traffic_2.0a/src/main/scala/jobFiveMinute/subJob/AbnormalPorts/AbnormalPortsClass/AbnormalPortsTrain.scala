package jobFiveMinute.subJob.AbnormalPorts.AbnormalPortsClass

import java.io.Serializable
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by TTyb on 2018/1/13.
  */
class AbnormalPortsTrain(properties: Properties, spark: SparkSession, dataFrame: DataFrame) extends LoggerSupport with Serializable {

  def trainMain(): Unit = {
    logger_ap.error("剔除标记数据")
    val resultData = judgeIP(dataFrame)
    val dataDF = getTarget()
    if (dataDF == null) {
      savePostgre(resultData)
    } else if (dataDF.take(10).length >= 1) {
      val code = (DstIP: String, DstPort: String, Proto: String) => {
        DstIP + "_" + DstPort + "_" + Proto
      }
      val addCol = udf(code)
      val aimData = dataDF.withColumn("Info", addCol(dataDF("ip"), dataDF("abnormalport"), dataDF("abnormalproto"))).select("Info")
      val resultDF = resultData.select("Info").union(aimData).distinct()
      savePostgre(resultDF)
    } else {
      savePostgre(resultData)
    }
  }

  //增加Info字段
  def addInfo(netData: DataFrame): DataFrame = {
    logger_ap.error("增加Info字段")
    val code = (DstIP: String, DstPort: String, Proto: String) => {
      DstIP + "_" + DstPort + "_" + Proto
    }
    val addCol = udf(code)
    val aimData = netData.withColumn("Info", addCol(netData("dstip"), netData("dstport"), netData("protocol")))
    aimData
  }

  //判断数据库中是否已经包含这个IP的信息
  def judgeIP(dataFrame: DataFrame): DataFrame = {
    logger_ap.error("获取训练集IP的信息")
    val lastTime = getDaytimeTime(-7)
    val nowTime = getDaytimeTime(1)

    val table = properties.getProperty("postgre.table.name.train")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select identityresult as Info from $table where type='端口异常' and standby01 is not null and standby01 <> '' " +
      s"and CAST(standby01 AS BIGINT) >= $lastTime " +
      s"and CAST(standby01 AS BIGINT) <= $nowTime) tt"
    val judgeIPDF = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", address)
      .option("dbtable", getIPSql)
      .option("user", username)
      .option("password", password)
      .load()
    val aimDF = dataFrame.select("Info")
    val baseDF = judgeIPDF.select("Info")
    val inequality = aimDF.except(baseDF)
    inequality
  }

  //保存结果到postgre
  def savePostgre(externalData: DataFrame): Unit = {
    logger_ap.error("正在train储存结果")
    externalData.show(2, false)
    externalData.foreachPartition {
      part =>
        val conn_str = properties.getProperty("postgre.address")
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              val infoArray = line.getAs[String]("Info").split("_")
              val ip = infoArray.head
              val port = infoArray.init.last
              val protocol = infoArray.last
              val table = properties.getProperty("postgre.table.name.train")
              val sqlText = s"""INSERT INTO $table (id, type, ip, dstip, dstport, typeresult, baselineresult, identityresult, standby01) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) """
              val prep = conn.prepareStatement(sqlText)
              prep.setString(1, ROWUtils.genaralROW())
              prep.setString(2, "端口异常")
              prep.setString(3, ip)
              prep.setString(4, ip)
              prep.setString(5, port)
              prep.setString(6, port)
              prep.setString(7, protocol)
              prep.setString(8, line.getAs[String]("Info"))
              prep.setString(9, (new Date().getTime.toString.substring(0, 10).toLong * 1000).toString)
              prep.executeUpdate
            } catch {
              case e: Exception => logger_ap.error("导入数据库出错" + e.getMessage)
            }
            finally {}
        }
        conn.close()
    }
  }

  //过滤1024以上的端口
  def filterPort(dataFrame: DataFrame): DataFrame = {
    val code = (port: String) => {
      var flag = 0
      if (port.toInt < 1024) {
        flag = 1
      }
      flag
    }

    val judgePort = org.apache.spark.sql.functions.udf(code)
    val transData = dataFrame.withColumn("flag", judgePort(dataFrame("dstport")))
    transData.filter("flag=1").drop("flag")
  }

  //获取前7天的时间long类型
  def getDaytimeTime(day: Int): Long = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    val newTime = fm.parse(fm.format(time))
    newTime.getTime
  }

  //获取hdfs的数据
  def getTarget(): DataFrame = {
    var dataDF: DataFrame = null
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time: Date = cal.getTime
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tim = fm.format(time)
    val hdfsPath = properties.getProperty("hdfs.path")
    val dataPath = hdfsPath + "/spark/target/thostattacks_" + tim + ".txt"
    try {
      dataDF = spark.read.json(dataPath).where("target='false' and modeltype='端口异常'").toDF()
    } catch {
      case e: Exception => logger_ap.error(e.getMessage)
    }
    dataDF
  }

}
