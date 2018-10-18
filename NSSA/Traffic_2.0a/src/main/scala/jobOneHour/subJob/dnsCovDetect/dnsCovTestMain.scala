package jobOneHour.subJob.dnsCovDetect

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.{Date, Properties}

import jobOneHour.subClass.{LoggerSupport, methodReadData}
import jobOneHour.subJob.dnsCovDetect.readHDFS.read
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
  * Created by Administrator on 2018/6/12.
  */
object dnsCovTestMain extends LoggerSupport{

//  算法组程序入口，调度不同(模型)类，本地只运行我的dnsCov模型
  def main(args: Array[String]): Unit = {

    //日志路径
    Logger.getLogger("org").setLevel(Level.ERROR)

    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/jobOneHour.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/jobOneHour.properties")))
    var properties: Properties = new Properties()
    properties.load(ipstream)

//    初始化spark
    val spark = getSparkSession(properties)

//    调用dnsCovDetect模型
    val time_read_data_begin = new Date().getTime //记录读数据开始运行时间（毫秒）
    val read_class = new methodReadData(spark, properties, logger)
    val data_dns = read(spark,properties)
    data_dns.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val dnsCov = new DNSCovMain(properties, spark, data_dns)
    dnsCov.main()
    val time_end = new Date().getTime //记录结束运行时间（毫秒）
    val time_read_data = (time_end - time_read_data_begin)/1000 //记录运行时间（毫秒）

  }

  //配置spark
  def getSparkSession(properties: Properties): SparkSession = {
    val sparkconf = new SparkConf()
      .setMaster(properties.getProperty("spark.master.url"))
      .setAppName(properties.getProperty("spark.app.name"))
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("spark.sql.shuffle.partitions"))
      .set("spark.sql.crossJoin.enabled", properties.getProperty("spark.sql.crossJoin.enabled"))
      .set("spark.sql.codegen", properties.getProperty("spark.sql.codegen"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }


}
