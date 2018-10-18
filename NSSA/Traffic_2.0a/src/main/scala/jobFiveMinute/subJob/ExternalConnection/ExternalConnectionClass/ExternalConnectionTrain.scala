package jobFiveMinute.subJob.ExternalConnection.ExternalConnectionClass

import java.io.Serializable
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobFiveMinute.subClass.LoggerSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by TTyb on 2018/1/13.
  */
class ExternalConnectionTrain(properties: Properties, spark: SparkSession, noConnection: DataFrame) extends LoggerSupport with Serializable {
  def trainMain(): Unit = {
    val baseData = getBaseData()
    val resultData = judgeIP(noConnection, baseData)
    val dataDF = getTarget()
    if (dataDF == null) {
      if (resultData.take(10).length >= 1) {
        savePostgre(resultData)
      }
    } else if (dataDF.take(10).length >= 1) {
      val code = (ip: String, abnormaltime: String) => {
        val newWord = abnormaltime
          .replace("9:00-12:00", "早")
          .replace("12:00-14:00", "中")
          .replace("14:00-18:00", "下")
          .replace("18:00-24:00", "晚")
          .replace("0:00-9:00", "凌")
        ip + "_" + newWord
      }
      val addCol = udf(code)
      val aimData = dataDF.withColumn("Info", addCol(dataDF("ip"), dataDF("abnormaltime"))).select("Info")
      val resultDF = resultData.select("Info").union(aimData).distinct()
      savePostgre(resultDF)
    } else {
      if (resultData.take(10).length >= 1) {
        savePostgre(resultData)
      }
    }
    logger_ec.error("train结束")
  }

  //判断数据库中是否已经包含这个IP的信息
  def judgeIP(dataFrame: DataFrame, baseData: DataFrame): DataFrame = {
    val netflowData = dataFrame
    //剔除掉数据库中不包含的字段
    val code = (SrcIP: String, moment: String) => {
      SrcIP + "_" + moment
    }
    val addCol = udf(code)
    val netData = netflowData.withColumn("Info", addCol(netflowData("srcip"), netflowData("moment")))
    val aimDF = netData.select("Info")
    val baseDF = baseData.select("Info")
    val inequality = aimDF.except(baseDF)
    inequality
  }

  //获取数据库中的基础数据
  def getBaseData(): DataFrame = {
    logger_ec.error("获取训练集IP的信息")
    val lastTime = getDaytimeTime(-7)
    val nowTime = getDaytimeTime(1)

    val table = properties.getProperty("postgre.table.name.train")
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    val getIPSql = s"(select identityresult as Info from $table where type='主动外联' and standby01 is not null and standby01 <> '' " +
      s"and CAST(standby01 AS BIGINT) >= $lastTime " +
      s"and CAST(standby01 AS BIGINT) <= $nowTime) tt"
    val baseData = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", address)
      .option("dbtable", getIPSql)
      .option("user", username)
      .option("password", password)
      .load()
    baseData
  }

  //保存结果到postgre
  def savePostgre(externalData: DataFrame): Unit = {
    logger_ec.error("正在储存train结果")
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
              val moment = infoArray.last
              val table = properties.getProperty("postgre.table.name.train")
              val sqlText = s"""INSERT INTO $table (id, type, ip, srcip, typeresult, baselineresult, identityresult, standby01) VALUES (?, ?, ?, ?, ?, ?, ?, ?) """
              val prep = conn.prepareStatement(sqlText)
              prep.setString(1, ROWUtils.genaralROW())
              prep.setString(2, "主动外联")
              prep.setString(3, ip)
              prep.setString(4, ip)
              prep.setString(5, moment)
              prep.setString(6, "0")
              prep.setString(7, line.getAs[String]("Info"))
              prep.setString(8, (new Date().getTime.toString.substring(0, 10).toLong * 1000).toString)
              prep.executeUpdate
            } catch {
              case e: Exception => logger_ec.error("导入数据库出错" + e.getMessage)
            }
            finally {}
        }
        conn.close()
    }
    logger_ec.error("储存train结果结束")
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
    logger_ec.error("获取HDFS带标签数据")
    var dataDF: DataFrame = null
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time: Date = cal.getTime
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tim = fm.format(time)
    val hdfsPath = properties.getProperty("hdfs.path")
    val dataPath = hdfsPath + "/spark/target/thostattacks_" + tim + ".txt"
    try {
      dataDF = spark.read.json(dataPath).where("target='false' and modeltype='主动外联'").toDF()
    } catch {
      case e: Exception => logger_ec.error(e.getMessage)
    }
    dataDF
  }

}
