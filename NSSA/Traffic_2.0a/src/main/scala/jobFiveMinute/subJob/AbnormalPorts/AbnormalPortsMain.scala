package jobFiveMinute.subJob.AbnormalPorts

import java.io.File
import java.util.Properties

import jobFiveMinute.subJob.AbnormalPorts.AbnormalPortsClass._
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import org.apache.log4j.Logger

/**
  * Created by TTyb on 2017/12/4.
  */
class AbnormalPortsMain(properties: Properties, spark: SparkSession, netData: DataFrame) extends LoggerSupport {
  //Main函数
  def main(): Unit = {
    //配置日志文件
    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_AbnormalPorts.properties")

    logger_ap.error("端口异常开始")
    try {
      val readyMain = new AbnormalPortsCheck(properties, spark, netData)
      readyMain.readyMain()
    } catch {
      case e: Exception => logger_ap.error(e.getMessage)
    }
    logger_ap.error("端口异常结束")

  }

}
