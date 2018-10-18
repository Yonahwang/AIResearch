package AssetStatistics

import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.{Calendar,Properties}
import AssetStatistics.SubClass.{changeAsset, idleAsset, outAsset, toLogger}

/**
  * Created by Yiutto on 2018年8月2日 0002.
  */
class assetStatsMain (mySparkSession: SparkSession, properties: Properties) extends toLogger {
  def main() = {
    // 发生时间，格式：yyyymmdd。
    val happen_day: String = getHappenday()

    // 1.变化资产 (可以将happen_day设为"20180719"作测试)
    val change_asset = new changeAsset(mySparkSession, properties, happen_day)
    logger.error("变化资产>>>>>>>>>>")
    change_asset.main()

    /*闲置资产和退网资产都用到了t_history_modelrules的"异常流量"相关历史数据，
      故可以将读取到的dataDF作为参数传至idleAsset,outAsset*/
    val historyDF = readTcpHistoryData(happen_day, "tcp")

    if (historyDF != null && historyDF.count() > 0) {
      // 加入缓存
      historyDF.persist()

      // 2.闲置资产(可以将happen_day设为"20180629"作测试)
      val idle_asset = new idleAsset(mySparkSession, properties, historyDF, happen_day)
      logger.error("闲置资产>>>>>>>>>>")
      idle_asset.main()

      // 3.退网资产(可以将happen_day设为"20180723"作测试)
      val out_asset = new outAsset(mySparkSession, properties, historyDF, happen_day)
      logger.error("退网资产>>>>>>>>>>")
      out_asset.main()

      // 释放缓存
      historyDF.unpersist()
    } else {
      logger.error("无相关历史数据，不能对（退网资产、闲置资产）进行检测")
    }
  }


  def getHappenday(): String = {
    val cal = Calendar.getInstance()

    //（当前时间的前一天作为detect day）
    cal.add(Calendar.HOUR, -24)
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val yesterday: String = dateFormat.format(cal.getTime)
    yesterday
  }

  def readTcpHistoryData(happenday: String, protocol: String) = {
    val postgresqlUsr = properties.getProperty("postgre.user")
    val postgresqlPwd = properties.getProperty("postgre.password")
    val postgresqlUrl = properties.getProperty("postgre.address")
    val postgresqlTable = properties.getProperty("postgre.table.name.train")

    val querySql =
      s"""
         |(SELECT temp.standby02 AS ips, temp.baselineresult AS all_data FROM $postgresqlTable AS temp
         | WHERE temp.standby03='$protocol' AND temp.typeresult LIKE '$happenday%'
         | AND temp.identityresult='ip_data' AND temp.type='异常流量'
         |) AS trafficdata""".stripMargin

    logger.error("读取Postgresql中表" + postgresqlUrl + "---" + postgresqlTable + "的信息,waiting........")
    val options = Map("driver" -> "org.postgresql.Driver", "url" -> postgresqlUrl, "dbtable" -> querySql,
      "user" -> postgresqlUsr, "password" -> postgresqlPwd)
    var dataDF: DataFrame = null
    try {
      dataDF = mySparkSession.read
        .format("jdbc")
        .options(options)
        .load()
        .distinct()
      logger.error("Sucess read Postgresql:" + postgresqlTable)
    } catch {
      case e: Exception => println("Error read Postgresql:" + e.getMessage)
    } finally {
    }

    dataDF
  }

}
