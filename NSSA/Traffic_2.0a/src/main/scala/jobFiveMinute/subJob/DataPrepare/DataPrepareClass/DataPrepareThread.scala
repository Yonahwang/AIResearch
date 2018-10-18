package jobFiveMinute.subJob.DataPrepare.DataPrepareClass

import java.io.FileWriter
import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
/**
  * Created by TTyb on 2018/1/10.
  */

class DataPrepareThread(properties: Properties, spark: SparkSession, filename:String) extends LoggerSupport with Runnable {
  override def run(): Unit = {
    val netflowPath = properties.getProperty("netflowHDFS")
    val httpPath = properties.getProperty("httpHDFS")
    val DNSPath = properties.getProperty("DNSHDFS")

    val netflowThread = new Thread(new PreparesThread(properties, spark, netflowPath, "netflow", filename))
    val httpThread = new Thread(new PreparesThread(properties, spark, httpPath, "http", filename))
    val dnsThread = new Thread(new PreparesThread(properties, spark, DNSPath, "dns", filename))

    netflowThread.start()
    httpThread.start()
    dnsThread.start()
  }

}

class PreparesThread(properties: Properties, spark: SparkSession, filePath: String, index: String, filename:String) extends LoggerSupport with Runnable {
  override def run(): Unit = {
    try {
      val Readymain = new DataPrepareHDFS(properties, spark)
      Readymain.readyMain(filePath, index, filename)
    } catch {
      case e: Exception => logger_dp.error("出错：" + e.getMessage)
    } finally {}
  }
}
