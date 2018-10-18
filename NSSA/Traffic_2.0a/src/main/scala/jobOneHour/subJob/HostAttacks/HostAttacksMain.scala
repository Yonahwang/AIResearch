package jobOneHour.subJob.HostAttacks

import java.io.File
import java.util.Properties

import jobOneHour.subJob.HostAttacks.HostAttacksClass.{HostAttacksReadyMain, LoggerSupport}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by TTyb on 2018/1/14.
  * 主机沦陷模型汇总
  */
class HostAttacksMain(properties: Properties, spark: SparkSession) extends LoggerSupport{
  def main(): Unit = {
    //配置日志文件
    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_HostAttacks.properties")

    logger.error("主机失陷抽取结果汇总开始")
    try {
      val hostAttacksReadyMain = new HostAttacksReadyMain(properties, spark)
      hostAttacksReadyMain.readyMain()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    logger.error("主机失陷抽取结果汇总结束")
  }

}
