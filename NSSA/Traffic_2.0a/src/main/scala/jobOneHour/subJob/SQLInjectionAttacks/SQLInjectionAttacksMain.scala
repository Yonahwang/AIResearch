package jobOneHour.subJob.SQLInjectionAttacks

import java.io.File
import java.util.Properties

import jobOneHour.subClass.LoggerSupport
import jobOneHour.subJob.SQLInjectionAttacks.SQLInjectionAttacksClass.{SQLInjectionAttacksReadyMain, SQLInjectionAttacksTrain}
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  * Created by TTyb on 2017/12/11.
  */
class SQLInjectionAttacksMain(properties: Properties, spark: SparkSession, httpData: DataFrame) extends LoggerSupport with Serializable{
  //main函数
  def main(): Unit = {
    //配置日志文件
//    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/log4j_SQLInjectionAttacks.properties")

    val index = properties.getProperty("indexSQLInjectionAttacks")
//    try {
      logger.error(index + "开始")
      if (index == "predict") {
        val sqlInjectionAttacksReadyMain = new SQLInjectionAttacksReadyMain(properties, spark, httpData)
        sqlInjectionAttacksReadyMain.readyMain()
      } else if (index == "train") {
        val sqlInjectionAttacksTrain = new SQLInjectionAttacksTrain(properties, spark, httpData)
        sqlInjectionAttacksTrain.saveTrainFile()
        logger.error(index + "结束")
      }
//    } catch {
//      case e: Exception => logger.error(e.getMessage)
//    } finally {}
  }
}
