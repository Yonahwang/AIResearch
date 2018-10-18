package jobOneDay.subJob.NetflowBotNet.NetflowBotNetClass

import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql.SparkSession

/**
  * Created by TTyb on 2018/2/12.
  */
class NetflowBotNetThread(properties: Properties, spark: SparkSession) extends Runnable with LoggerSupport {
  override def run(): Unit = {
    while (true) {
      try {
        val cal = Calendar.getInstance()
        cal.setTime(new Date())
        val hour = cal.get(Calendar.HOUR_OF_DAY)
        logger.error("netflow的僵尸网络开始")
        //如果当前是0点就删除昨天的数据
        if (hour == 0) {
          val netflowBotNetReadyMain = new NetflowBotNetReadyMain(properties,spark)
          netflowBotNetReadyMain.readyMain()
        }
      } catch {
        case e: Exception => logger.error(e.getMessage)
      }
      logger.error("netflow的僵尸网络结束，暂停1小时")
      Thread.sleep(3600000)
    }
  }
}
