package AssetStatistics.SubClass

import java.util.Properties
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by Yiutto on 2018年7月31日 0031.
  */
class outAsset(sparksession: SparkSession, properties: Properties,
               historyDF: DataFrame, happen_day: String) extends funcMethod with Serializable {

  def main() = {


    // 读取历史周期（默认为30天）
    val day_threshold = properties.getProperty("out.day.threshold").toInt
    // 退网资产的flag(3)
    val out_flag = properties.getProperty("out.flag").toInt

    // 当前检测出“待检测退网资产”,out_assetDF1的字段为("ip1", "happen_day1")
    val out_assetDF1: DataFrame = getUndetectedOutAsset(historyDF, happen_day)
    // 从t_history_modelrules读取（type=‘待检测退网资产 && identityresult=‘history_asset’）
    // out_assetDF2的字段为("ip2", "happen_day2")
    val out_assetDF2: DataFrame = readPostgresqlUndetecedAssetIP(sparksession, properties, "待检测退网资产")
    if (out_assetDF2 != null || out_assetDF2.count() > 0) {
      // out_assetDF1和out_assetDF2对比，如果happen_day1-happen_day2大于等于day_threshold，则认为该ip为退网资产ip
      val out_assetRDD: RDD[(String, Int)] = getIdleOrOutIP(out_assetDF1, out_assetDF2, day_threshold, out_flag)
      // 将闲置资产入库
      assetToPostgresql(out_assetRDD, happen_day, properties)
    }
    // 将out_assetDF1更新入库t_history_modelrules
    updatePostgresqlUndetecedAssetIP(sparksession, out_assetDF1, properties, "待检测退网资产")
  }


  /**
    * 得到“待检测退网资产”的ip
    *
    * @param historyDF
    * @param  happen_day
    * @return
    */
  def getUndetectedOutAsset(historyDF: DataFrame, happen_day: String): DataFrame = {
    import sparksession.implicits._
    val out_assetDF = historyDF.rdd.flatMap { line =>
      line.getAs[String]("ips").split("#")
    }.map(f => (f, happen_day)).toDF("ip1", "happen_day1")

    out_assetDF
  }

}
