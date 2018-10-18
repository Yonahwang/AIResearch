package jobOneDay.subJob.CleanData

import java.io.File
import java.util.{Calendar, Date, Properties}

import jobOneDay.subJob.CleanData.CleanDataClass.CleanDataHDFS
import org.apache.spark.sql.SparkSession
import jobOneHour.subClass.LoggerSupport
import org.apache.log4j.PropertyConfigurator

import scala.util.Try

/**
  * Created by TTyb on 2018/1/20.
  */
class CleanDataMain(properties: Properties, spark: SparkSession) extends LoggerSupport {
  def main(): Unit = {
    //配置日志文件
    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_CleanData.properties")
    try {
      logger.error("删除数据开始")
      val cleanDataHDFS = new CleanDataHDFS(properties, spark)
      cleanDataHDFS.readyMain()
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
    logger.error("删除数据结束")
  }

}
