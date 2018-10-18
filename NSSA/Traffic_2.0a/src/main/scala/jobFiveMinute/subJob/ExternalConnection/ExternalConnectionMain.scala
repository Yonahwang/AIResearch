package jobFiveMinute.subJob.ExternalConnection

import java.io.File
import java.util.Properties

import jobFiveMinute.subJob.ExternalConnection.ExternalConnectionClass._

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import org.apache.log4j.Logger

/**
  * Created by TTyb on 2017/12/6.
  * 主动外联模型
  */
class ExternalConnectionMain(properties: Properties, spark: SparkSession, netData: DataFrame) extends LoggerSupport {
  //Main函数
  def main(): Unit = {
    //配置日志文件
    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_ExternalConnection.properties")
    logger_ec.error("主动外联开始")
    try {
      val readyMain = new ExternalConnectionCheck(properties, spark, netData)
      readyMain.readyMain()
    } catch {
      case e: Exception => logger_ec.error(e.getMessage)
    }
    logger_ec.error("主动外联结束")
  }

  //配置spark
  def getSparkSession(properties: Properties): SparkSession = {
    val masterUrl = properties.getProperty("spark.master.url")
    val appName = properties.getProperty("spark.app.name")
    val sparkconf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName(appName)
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("spark.sql.shuffle.partitions"))
      .set("spark.sql.crossJoin.enabled", "true")
      .set("spark.sql.codegen", "true")
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }
}
