package jobOneHour.subJob.HostAttacks.HostAttacksClass

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by TTyb on 2018/1/17.
  */
class HostAttacksThread(properties: Properties, spark: SparkSession) extends Runnable with LoggerSupport {
  override def run(): Unit = {
    while (true) {
      logger.error("主机失陷抽取结果汇总开始")
      try {
        val hostAttacksReadyMain = new HostAttacksReadyMain(properties, spark)
        hostAttacksReadyMain.readyMain()
      } catch {
        case e: Exception => logger.error(e.getMessage)
      }
      val timeSleep = properties.getProperty("timeSleep")
      logger.error("主机失陷抽取结果汇总结束，暂停" + timeSleep + "毫秒")
      Thread.sleep(timeSleep.toInt)
    }
  }
}
