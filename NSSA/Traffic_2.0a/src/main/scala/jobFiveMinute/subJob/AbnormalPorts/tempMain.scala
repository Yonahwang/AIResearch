package jobFiveMinute.subJob.AbnormalPorts

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by TTyb on 2018/6/14.
  */
object tempMain {
  def main(args: Array[String]): Unit = {
    //读取配置文件
    val properties: Properties = new Properties()
    val ipstream = new BufferedInputStream(new FileInputStream(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/jobFiveMinute.properties"))
    properties.load(ipstream)
    val spark = getSparkSession(properties)
    val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://10.130.10.41:9000//spark/data/netflow/201807021025.txt")
    val netData = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
    val abnormalPortsMain = new AbnormalPortsMain(properties,spark,netData)
    abnormalPortsMain.main()
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
