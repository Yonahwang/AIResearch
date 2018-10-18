package AssetStatistics.SubClass



import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Range
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Yiutto on 2018年7月13日 0013.
  */
class idleAsset(sparksession: SparkSession, properties: Properties,
                historyDF: DataFrame, happen_day: String) extends funcMethod with Serializable {
  def main() = {
    // 读取网元ip(考虑到每次网元ip可能随时增加删除)将目标ip进行排序，方便后期通过ip映射到tcp_bz
    val targetipArr: Array[String] = getIpSet(properties).sorted.toArray
    // 获取流量阈值(默认1kb=1024)
    val byte_threshold: Double = properties.getProperty("netflow.threshold").toDouble
    // 获取天数阈值（默认为3day）
    val day_threshold = properties.getProperty("idle.day.threshold").toInt
    // 闲置资产的flag(5)
    val idle_flag = properties.getProperty("idle.flag").toInt

    val day_traffic_dataArr: Array[Array[Double]] = historyDF.rdd.map { line =>
      val ips = line.getAs[String]("ips")
      val all_data = line.getAs[String]("all_data")
      val ips_data: Array[Double] = getAllIpData((ips, all_data), targetipArr)
      ips_data
    }.collect()

    // 得到行列式数组，总行数为ip的总个数，列数为288（默认1天288个5min）
    val ip_traffic_dataArr: Array[Array[Double]] = transposeDouble(day_traffic_dataArr)
    // 当前检测出“待检测闲置资产”,idle_assetDF1的字段为("ip1", "happen_day1")
    val idle_assetDF1: DataFrame = getUndetecedIdleIP(ip_traffic_dataArr, byte_threshold, happen_day, targetipArr)
    if (idle_assetDF1 != null){
      // 从t_history_modelrules读取（type=‘待检测闲置资产 && identityresult=‘history_asset’）
      // idle_assetDF2的字段为("ip2", "happen_day2")
      val idle_assetDF2: DataFrame  = readPostgresqlUndetecedAssetIP(sparksession, properties, "待检测闲置资产")
      if (idle_assetDF2 != null && idle_assetDF2.count() > 0){
        // idle_assetDF1和idle_assetDF2对比，如果happen_day1-happen_day2大于等于day_threshold，则认为该ip为闲置资产ip
        val idle_assetRDD: RDD[(String, Int)] = getIdleOrOutIP(idle_assetDF1, idle_assetDF2, day_threshold, idle_flag)
        // 将闲置资产入库
        assetToPostgresql(idle_assetRDD, happen_day, properties)
      }

    // 将idle_assetDF1更新入库t_history_modelrules
      updatePostgresqlUndetecedAssetIP(sparksession, idle_assetDF1, properties, "待检测闲置资产")
    }




  }

  /**
    * 得到“待检测闲置资产”的ip
    * DataFrame的字段(ip1, happen_day1)
    * @param ip_traffic_dataArr
    * @param byte_threshold
    * @param happen_day
    * @param targetipArr
    * @return
    */
  def getUndetecedIdleIP(ip_traffic_dataArr: Array[Array[Double]], byte_threshold: Double,
                happen_day:String, targetipArr: Array[String]): DataFrame = {
    val idleIPArr = new ArrayBuffer[(String, String)]()
    for (i <- Range(0, ip_traffic_dataArr.size)) {
      // 获取单个ip当天的tcp流量值
      val ip_day_data: Array[Double] = ip_traffic_dataArr(i)
      // 统计单个ip当天值为Nan的个数
      val Nan_counts = ip_day_data.count(_.isNaN)
      // 统计单个ip当天值小于byte_threshold的个数
      val threshold_counts = ip_day_data.count(_ < byte_threshold)
      // 如果当天的值要么为Nan，要么小于byte_threshold，则认为该ip为闲置资产
      if (Nan_counts + threshold_counts == ip_day_data.size) {
        // 5表示闲置资产
        idleIPArr += Tuple2(targetipArr(i), happen_day)
      }
    }

    var idle_assetDF: DataFrame = null

    if (idleIPArr.toArray.size > 0){
      import sparksession.implicits._
      idle_assetDF = sparksession.sparkContext.makeRDD(idleIPArr.toArray).toDF("ip1", "happen_day1")
    }

    idle_assetDF
  }




  /**
    * 根据网元ip数组来提取dataframe中ip的相关数据
    *
    * @param ips_data
    * @param targetipArr
    * @return
    */
  def getAllIpData(ips_data: (String, String), targetipArr: Array[String]): Array[Double] = {
    // 将ips和all_data一一映射
    val ips: Array[String] = ips_data._1.split("#")
    val all_data = ips_data._2.split("#")
    val ips_dataDict = new mutable.HashMap[String, Double]()
    val len = ips.length
    for (i <- Range(0, len)) {
      ips_dataDict(ips(i)) = all_data(i).toDouble
    }

    // 将网元ip的tcp_bz按之前排列的ip顺序整合成一个字符串，方便入库
    val all_dataArr = {
      for (ip <- targetipArr) yield {
        ips_dataDict.getOrElse(ip, Double.NaN)
      }
    }

    all_dataArr
  }

  /**
    * 读取postgrel的网元ip
    *
    * @param properties
    * @return
    */
  def getIpSet(properties: Properties): List[String] = {

    val deviceIPTable = properties.getProperty("asset.table")
    val query = s"(SELECT device_ip FROM $deviceIPTable where status<>2)"
    val driver = s"org.postgresql.Driver"
    val address = properties.getProperty("postgre.address")
    val username = properties.getProperty("postgre.user")
    val password = properties.getProperty("postgre.password")
    logger.error("获取网元ip" + address + ">>>>" + deviceIPTable)
    Class.forName(driver)
    var ipSet = List("")
    val posgreConn: Connection = DriverManager.getConnection(address, username, password)
    try {
      val st: Statement = posgreConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
      val rs: ResultSet = st.executeQuery(query)

      ipSet = ipSet.drop(1)
      while (rs.next()) {
        val ip = rs.getString(1)
        ipSet = ip :: ipSet
      }
    } catch {
      case e: Exception => println("Error read:" + e.getMessage)
    } finally {
      posgreConn.close()
    }

    ipSet.distinct
  }

}
