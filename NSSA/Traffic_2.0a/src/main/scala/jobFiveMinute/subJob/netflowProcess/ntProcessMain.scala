package jobFiveMinute.subJob.netflowProcess

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import jobFiveMinute.subJob.netflowProcess.classmethod.corrFunc
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

//import jobFiveMinute.subJob.netflowProcess.classmethod.corrFunc
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by Yiutto on 2018年6月15日 0015.
  */

/**
  *
  * @param dataDF
  * @param filename "hdfs://10.130.10.41:9000/spark/data/netflow/201806141435.txt"
  * @param mysparksession
  * @param properties
  */
class ntProcessMain(dataDF: DataFrame, filename: String, mysparksession: SparkSession, properties: Properties)
  extends corrFunc with Serializable {

  def main(): Unit = {

    //A.读取配置文件
    //（用于服务器Linux）
    //    val filePath = new File("..").getAbsolutePath
    val filePath = System.getProperty("user.dir")
    //log日志文件路径
    PropertyConfigurator.configure(filePath + "/conf/netflowProcess_log4j.properties")

    // 201806130445
    val happen_time: String = get_happen_time(filename)

    try {
      //当前文件名和先前文件名不相等时，成立
      // 对当前时间生成的文件进行特征处理工程,转换为(pro, ips, all_ip_data)
      val netflow_dataArr: Array[(String, String, String)] = PerfiveMinutesTCP(dataDF)
      if (netflow_dataArr.size == 0) {
        logger_np.error(happen_time + "时刻ips对应的data为null, 没有必要入库！")
      } else {
        // 如果存在数据，入库(3种协议tcp、udp、icmp)
        netflowSaveToPostgrel(happen_time, netflow_dataArr)
      }
    } catch {
      case e: Exception => logger_np.error("Error：" + e.getMessage)
    } finally {}

  }

  /**
    * "hdfs://10.130.10.41:9000/usr/hadoop/nta/netflow/netflow201806131345.txt"提取出"201806131345"
    * 从文件的路径中提取出发生时间
    *
    * @param filename
    * @return
    */
  def get_happen_time(filename: String): String = {
    val len = filename.length
    val happen_time = filename.substring(len - 16, len - 4)
    happen_time
  }


  /**
    * 聚合5分钟内指定ip的netflow的相关数据（bytesize）
    *
    * @param dataDF
    * @return
    */

  def PerfiveMinutesTCP(dataDF: DataFrame): Array[(String, String, String)] = {
    import mysparksession.implicits._

    val protoArr = Array("tcp", "udp", "icmp")
    // 读取targetip数据
    val targetipDF: DataFrame = getIpSet(properties).toDF("ip")
    // 定义ips_data初始值
    var ips_data: (String, String) = ("", "")
    // 定义netflow（保存3种协议）
    val netflowArr = new ArrayBuffer[(String, String, String)]
    // 只有DataFrame不为null，使用createOrReplaceTempView才不会报“java.lang.NullPointerException”
    if (!(dataDF == null || targetipDF == null)) {
      dataDF.createOrReplaceTempView("flow")
      targetipDF.createOrReplaceTempView("iptable")

      // 分3种协议去遍历
      for (proto <- protoArr) {
        val protoDF = mysparksession.sql(query_sql(proto))
        if (protoDF.count() > 0) {
          val ip_protoDataRow: RDD[(String, String)] = protoDF.rdd.map { line =>
            (line.getAs[String]("ip"), line.getAs[Double]("tcp_bz").formatted("%.2f").toString)
          }
          ips_data = ip_protoDataRow.reduce { (x, y) =>
            (x._1 + "#" + y._1, x._2 + "#" + y._2)
          }
          netflowArr += Tuple3(proto, ips_data._1, ips_data._2)
        }
      }
    }

    netflowArr.toArray
  }

  def query_sql(proto: String) = {
    val sql: String =
      s"""
         |SELECT *
         |FROM
         |   (SELECT ip, sum(byte) AS tcp_bz
         |    FROM
         |         (SELECT srcip AS ip, sum(upbytesize) AS byte
         |          FROM flow WHERE protocol= '$proto' GROUP BY srcip
         |          UNION ALL
         |          SELECT dstip AS ip, sum(downbytesize) AS byte
         |          FROM flow WHERE protocol= '$proto' GROUP BY dstip)
         |    GROUP BY ip)
         |   AS temp
         |WHERE temp.ip IN (SELECT ip FROM iptable)
      """.stripMargin
    sql
  }

  /**
    * 将处理后的netflow相关数据保存至Postgrel
    *
    * @param happen_time
    * @param netflow_dataArr
    */
  def netflowSaveToPostgrel(happen_time: String, netflow_dataArr: Array[(String, String, String)]) = {
    // 设置postgresql配置参数
    val table = properties.getProperty("postgre.table.name.train")
    val conn_url = properties.getProperty("postgre.address")
    Class.forName("org.postgresql.Driver").newInstance
    val conn = DriverManager.getConnection(conn_url, properties.getProperty("postgre.user"),
      properties.getProperty("postgre.password"))

    logger_np.error("准备导入" + conn_url + "=>" + table)
    netflow_dataArr.foreach {
      line =>
        try {
          val (protocol, ips, all_ip_data) = line
          val sqlText =
            s"""INSERT INTO $table (id, type, typeresult, baselineresult, identityresult,
               |standby01, standby02, standby03)
               |VALUES (?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
          val prep = conn.prepareStatement(sqlText)
          prep.setString(1, generateID())
          prep.setString(2, "异常流量")
          prep.setString(3, happen_time)
          prep.setString(4, all_ip_data)
          prep.setString(5, "ip_data")
          prep.setString(6, getNowTime.toString)
          prep.setString(7, ips)
          prep.setString(8, protocol)
          prep.executeUpdate
          logger_np.error("Successed: " + happen_time + "->" + protocol + " --此条记录入库成功")
        } catch {
          case e: Exception => logger_np.error("导入出错" + e.getMessage)
        } finally {}

    }
    conn.close
  }


}
