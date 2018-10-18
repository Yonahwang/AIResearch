package jobFiveMinute.subJob.DataPrepare

import java.io.File
import java.util.Properties

import jobFiveMinute.subJob.DataPrepare.DataPrepareClass.DataPrepareThread
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * Created by TTyb on 2018/1/9.
  * 数据预处理程序
  */
class DataPrepareMain (properties: Properties, spark: SparkSession, filename:String) {
  def main(): Unit = {
    //配置日志文件
    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_DataPrepare.properties")

    val threadRun = new Thread(new DataPrepareThread(properties,spark, filename))
    threadRun.run()
  }
}
