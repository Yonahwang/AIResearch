package jobOneHour.subJob.MaliciousCode

import java.io.File
import java.util.Properties

import jobOneHour.subJob.HostAttacks.HostAttacksClass.LoggerSupport
import jobOneHour.subJob.MaliciousCode.MaliciousCodeClass.{MaliciousCodeReadyMain, MaliciousCodeTrain}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try


/**
  * Created by TTyb on 2017/9/27.
  */
class MaliciousCodeMain(properties: Properties, spark: SparkSession, httpData: DataFrame) extends LoggerSupport with Serializable{

  //Main函数
  def main(): Unit = {
    //配置日志文件
//    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_MaliciousCode.properties")
    val index = properties.getProperty("indexMaliciousCode")
    try {
      logger.error(index + "开始")
      if (index == "predict") {
        val maliciousCodeReadyMian = new MaliciousCodeReadyMain(properties, spark, httpData)
        maliciousCodeReadyMian.readyMain()
      } else if (index == "train") {
        val maliciousCodeTrain = new MaliciousCodeTrain(properties, spark, httpData)
        maliciousCodeTrain.saveTrainFile()
      }
      logger.error(index + "结束")

    } catch {
      case e: Exception => logger.error(e.getMessage)
    } finally {}
  }

}
