package jobOneHour.subJob.trafficAnomaly

import java.net.InetAddress
import java.io.File
import scala.collection.mutable.ListBuffer
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Properties}

import jobOneHour.subJob.trafficAnomaly.classmethod.{corrFuction, anomalyProtoTraffic}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import jobOneHour.subClass.saveToKAFKA
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.Range
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Yiutto on 2018年7月02日 0014.
  */
/**
  *
  * @param dataDF
  * @param happentimeH (示例："2018061412")只能处理当前时刻的一小时的数据(12:00-13:00)(格式为yyyyMMddHH)
  * @param mysparksession
  * @param properties
  */
class anomalyTrafficMain(dataDF: DataFrame, happentimeH: String, mysparksession: SparkSession, properties: Properties)
  extends corrFuction with saveToKAFKA with Serializable {

  def main(): Unit = {
    //A.读取配置文件
    //（用于服务器Linux）
    val filePath = new File("..").getAbsolutePath
    //    val filePath = System.getProperty("user.dir")
    //log日志文件路径
    PropertyConfigurator.configure(filePath + "/conf/trafficAnomaly_log4j.properties")

    val kafka_node = properties.getProperty("kafka.nodes")
    val kafka_topic = properties.getProperty("kafka.topic2")

    val sc = mysparksession.sparkContext
    try {

      // 读取网元ip(考虑到每次网元ip可能随时增加删除)将目标ip进行排序，方便后期通过ip映射到tcp_bz
      val targetipArr: Array[String] = getIpSet(properties).sorted.toArray
      // 对原始netflow数据作处理, 得到("srcip", "dstip", "source", "position", "protocol", "proto7")
      val sourceDF: DataFrame = traceNetflowSource(dataDF)

      // 将其整合成广播变量
      val targetipArrB: Broadcast[Array[String]] = sc.broadcast(targetipArr)
      val sourceDFB: Broadcast[DataFrame] = sc.broadcast(sourceDF)

      // 将所有协议整合一起跑
      val protoArr = Array("tcp", "udp", "icmp")
      val eventDFListBuffer = new ListBuffer[DataFrame]()

      val anomaly_proto_netflow = new anomalyProtoTraffic(mysparksession, properties)

      for (proto <- protoArr) {
        val proto_anomalyDF = anomaly_proto_netflow.anomalyProtoMain(targetipArrB.value, sourceDFB.value, happentimeH, proto)
        if (proto_anomalyDF != null) {
          eventDFListBuffer += proto_anomalyDF
        }
      }

      if (eventDFListBuffer.length != 0) {
        val eventDF = mergeDataFrame(eventDFListBuffer.toList)
        val (insert_nums, kafkaDF) = makeKafkaNetflowData(eventDF, happentimeH)
        logger.error("有" + insert_nums + "条数据入kafka")
        toKafka(mysparksession, (kafka_node, kafka_topic), kafkaDF)
      } else {
        logger.error("没有相关异常的数据入kafka")
      }

    } catch {
      case e: Exception => logger.error("Error：" + e.getMessage)
    } finally {
    }

  }

  //合并dataframe函数
  def mergeDataFrame(dataFrameList: List[DataFrame]): DataFrame = {
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val unionData = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    unionData
  }


  /**
    * 将netflow数据进行整合，增加溯源字段(5元组srcip, dstip, srcport, dstport, protocol)
    * 将netflow数据进行整合，增加地理位置字段(5元组scountry, sprovince, scity, slatitude, slongitude)
    *
    * @param encryptedPort
    * @return
    */
  def traceNetflowSource(encryptedPort: DataFrame): DataFrame = {
    val eventSourceUdf = org.apache.spark.sql.functions.udf(
      (srcip: String, dstip: String, srcport: String, dstport: String, protocol: String) => {
        srcip + "," + dstip + "," + srcport + "," + dstport + "," + protocol
      }
    )

    // 将源目的ip的地理位置整合成一个字段
    val positionUdf = org.apache.spark.sql.functions.udf(
      (scountry: String, sprovince: String, scity: String, slatitude: String, slongitude: String,
       dcountry: String, dprovince: String, dcity: String, dlatitude: String, dlongitude: String) => {
        scountry + "," + sprovince + "," + scity + "," + slatitude + "," + slongitude + "," +
          dcountry + "," + dprovince + "," + dcity + "," + dlatitude + "," + dlongitude
      }
    )

    val DF = encryptedPort.withColumn("source", eventSourceUdf(encryptedPort("srcip"), encryptedPort("dstip"),
      encryptedPort("srcport"), encryptedPort("dstport"), encryptedPort("protocol")))
      .withColumn("position", positionUdf(encryptedPort("scountry"), encryptedPort("sprovince"),
        encryptedPort("scity"), encryptedPort("slatitude"), encryptedPort("slongitude"),
        encryptedPort("dcountry"), encryptedPort("dprovince"), encryptedPort("dcity"),
        encryptedPort("dlatitude"), encryptedPort("dlongitude")))

    DF.select("srcip", "dstip", "source", "position", "protocol", "proto7").distinct()
  }


  def makeKafkaNetflowData(eventSourceDF: DataFrame, process_current_timeH: String): (Long, DataFrame) = {
    //      ("anomaly_ip", "happen_time", "evidences", "sources", "traffics", "baselines",
    //        "positions", "encrypted", "anomaly_min", "anomaly_value", "protocol")
    import mysparksession.implicits._

    val kafkaDF = eventSourceDF.rdd.map {
      line =>
        val id: String = generateID
        // 网元ip
        val ip: String = line.getAs[String]("anomaly_ip")
        // 事件类型
        val modeltype: String = "异常流量"
        // 入库时间
        val resulttime: Long = getNowTime
        // 异常流量的相关证据
        val evidence = line.getAs[String]("evidences")
        // 异常ip的五元组
        val eventsource = line.getAs[String]("sources")
        // 是否造成主机沦陷0/1
        val happen: Int = 0
        // 1h的实际流量（12个5min）
        val normaltime: String = line.getAs[String]("traffics")
        // 1h的流量基线（12个5min）
        val abnormaltime: String = line.getAs[String]("baselines")
        // 访问dga的时间段
        val abnormalport: String = getDateTime(process_current_timeH)
        val abnormalproto = ""
        val abnormaltype = ""
        val hisbaseline = ""
        // 1h内的异常时间（精确min）
        val standby01 = line.getAs[String]("anomaly_min")
        // 1h内的异常值
        val standby02 = line.getAs[String]("anomaly_value")
        // 异常协议
        val standby03 = line.getAs[String]("protocol")
        val event_rule_id = "MODEL_ABNORMALTRAFFIC_MV1.0_001_008"
        val proto = standby03
        // 本机的ip，即上报ip
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_sub_type = "AttackBasic6B8"
        // 源ip和目的ip的地理位置信息
        val src_position: Array[String] = line.getAs[String]("positions").split("#")(0).split(",").slice(0, 5)
        val dst_position: Array[String] = src_position.clone()
        val position = (src_position ++ dst_position).mkString("#")

        // 原始日志
        val original_log = "{\"网元IP\":\"" + ip + "\"," +
          "\"五元组（源IP，目的IP，源端口，目的端口，协议）\":\"" + eventsource + "\"," +
          "\"模型类型\":\"" + modeltype + "\"," +
          "\"发生时间（1h内）\":\"" + abnormalport + "" + "\"," +
          "\"上报IP\":\"" + reportneip + "\"," +
          "\"实际流量（1小时内每5min）\":\"" + normaltime + "\"," +
          "\"流量基线（1小时内每5min）\":\"" + abnormaltime + "\"}"
        val encrypted = line.getAs[String]("encrypted")
        val event_flag = "0"

        val srcip = ip
        val dstip = ip
        val srcport = ""
        val dstport = ""

        eventRes(id, ip, modeltype, resulttime, evidence, eventsource, happen, normaltime, abnormaltime, abnormalport,
          abnormalproto, abnormaltype, hisbaseline, standby01, standby02, standby03, event_rule_id, proto, reportneip,
          event_sub_type, position, original_log, encrypted, event_flag, srcip, dstip, srcport, dstport)
    }.toDF()

    (kafkaDF.count(), kafkaDF)
  }

}

case class eventRes(id: String, ip: String, modeltype: String, resulttime: Long, evidence: String, eventsource: String,
                    happen: Int, normaltime: String, abnormaltime: String, abnormalport: String, abnormalproto: String,
                    abnormaltype: String, hisbaseline: String, standby01: String, standby02: String, standby03: String,
                    event_rule_id: String, proto: String, reportneip: String, event_sub_type: String, position: String,
                    original_log: String, encrypted: String, event_flag: String, srcip: String, dstip: String, srcport: String,
                    dstport: String)
