package AssetStatistics.SubClass

import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Yiutto on 2018年7月13日 0013.
  */
class changeAsset(sparksession: SparkSession, properties: Properties, happen_day: String) extends funcMethod
  with Serializable{

  def main() = {

    // 连接ES查找（端口异常、异常流量）
    val anomaly_event = readEventsFromES(happen_day)

    // 设置资产变化ip发生event种类的阈值 （端口异常、异常流量）
    // 默认设为1
    val anomaly_event_kinds = properties.getProperty("anomaly_event_kinds").toInt

    if (anomaly_event != null ){
      //    anomaly_event.show(false)
      val change_assetRDD: RDD[(String, Int)] = getChangeAsset(anomaly_event, anomaly_event_kinds)

      // 将all_anomaly_eventRDD的数据入库
      assetToPostgresql(change_assetRDD, happen_day, properties)
    } else {
      logger.error("异常事件为空，无资产变化的ip")
    }
  }

  def getChangeAsset(anomaly_event: DataFrame, anomaly_event_kinds:Int): RDD[(String, Int)] = {
    val eventRDD: RDD[(String, Iterable[String])] = anomaly_event.rdd.map { line =>
      val anomaly_ip = line.getAs[String]("anomaly_ip")
      val anomaly_event = line.getAs[String]("event_rule_name")
      (anomaly_ip, anomaly_event)
    }.groupByKey()

    val change_assetRDD: RDD[(String,  Int)] = eventRDD.map { line =>
      var anomaly_flag = 0
      // 异常事件的种类超过anomaly_event_kinds的值，则认为改ip为变化资产
      if (line._2.size > anomaly_event_kinds) {
        //变化资产的标识符为"4"
        anomaly_flag = 4
      }
      (line._1,  anomaly_flag)
    }.filter(f => f._2 == 4)

    change_assetRDD
  }




  /**
    * 读取当天发生的安全事件（异常流量、端口异常）的异常ip
    *
    * @return
    */
  def readEventsFromES(day: String): DataFrame = {

    val esPath = "event_" + day + "/" + "event_" + day
    var eventDataDF: DataFrame = null
    try {
      val esData = sparksession.read.format("org.elasticsearch.spark.sql").load(esPath)
      esData.createOrReplaceTempView("event_table")
      eventDataDF = sparksession.sql(
        s"""
           | SELECT destip AS anomaly_ip, event_rule_name
           | FROM event_table
           | WHERE event_rule_name='端口异常' OR event_rule_name='异常流量'
      """.stripMargin).distinct()
    } catch {
      case e: Exception => logger.error("Error read:" + e.getMessage)
    } finally {}

    eventDataDF
  }

}
