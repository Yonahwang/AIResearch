package jobOneHour.subJob.Heartbeat

import java.util.Properties
import jobOneHour.subClass._
import org.apache.spark.sql._
import org.apache.log4j.Logger

class heartbeatThread(spark: SparkSession,data:DataFrame,properties:Properties, logger:Logger) extends Runnable {
  override def run(): Unit = {
    while (true) {
      logger.error("开始检测>>>>>>>>>>>>")
      val time1 = System.currentTimeMillis()
      val heartbeatPredict = new heartbeat_autocorrelation(spark,data,properties).heartbeatPredict()
      val time2 = System.currentTimeMillis()
      logger.error("检测用时为" + (time2 - time1) / 1000 + "秒")
      Thread.sleep(3600000)
    }
  }
}
