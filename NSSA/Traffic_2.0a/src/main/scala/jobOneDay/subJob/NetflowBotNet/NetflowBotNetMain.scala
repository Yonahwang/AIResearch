package jobOneDay.subJob.NetflowBotNet

import java.io.File

import jobOneDay.subClass.LoggerSupport
import java.util.{Calendar, Date, Properties}

import jobOneDay.subJob.NetflowBotNet.NetflowBotNetClass.NetflowBotNetReadyMain
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by TTyb on 2018/2/2.
  */
class NetflowBotNetMain(properties: Properties, spark: SparkSession) extends LoggerSupport {
  def main(): Unit = {
    //配置日志文件
    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_NetflowBotNet.properties")

    try {
      logger.error("netflow的僵尸网络开始")
      val netflowBotNetReadyMain = new NetflowBotNetReadyMain(properties, spark)
      netflowBotNetReadyMain.readyMain()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
  }
}
