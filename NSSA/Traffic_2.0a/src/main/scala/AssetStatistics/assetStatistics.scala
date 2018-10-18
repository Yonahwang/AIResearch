package AssetStatistics

import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger, PropertyConfigurator}

import scala.util.Try


/**
  * Created by Administrator on 2018年8月2日 0002.
  */
object assetStatistics {
  def main(args: Array[String]): Unit = {

    //读取log4j文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf) (主要是为了能在Linux和Windows下运行)
    val configureStream=
      Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/log4j_asset_statistics.properties")))
        .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/log4j_asset_statistics.properties")))
    PropertyConfigurator.configure(configureStream)

    val fileProperties = new Properties()
    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    val propertiesStream =
      Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/asset_statistics.properties")))
        .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/asset_statistics.properties")))
    fileProperties.load(propertiesStream)

    while (true) {
      val time = get_day(format = "HH:mm")
      val flag: Boolean = (time == fileProperties.getProperty("run.time"))
      if(flag){
        //B.建立spark连接
        val mySparkSession = createSparkSession(fileProperties)
        val asset_model = new assetStatsMain(mySparkSession, fileProperties)
        asset_model.main()

        println("检测完毕")
        mySparkSession.stop()  //关闭spark连接
        Thread.sleep(60000) //1分钟
      }
    }


  }

  //获取当前时间(String)
  def get_day(format:String = "yyyy-MM-dd HH:mm:ss") = {
    val time_long = new Date().getTime
    val day = new SimpleDateFormat(format).format(time_long)
    day
  }

  //     1. Creating sql.SparkSession
  def createSparkSession(properties: Properties) = {
    val sparkconf = new SparkConf()
      .setMaster(properties.getProperty("spark.master.url"))
      .setAppName(properties.getProperty("spark.app.name"))
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))

    val mySparkSession = SparkSession.builder().config(sparkconf).getOrCreate()

    mySparkSession
  }

}
