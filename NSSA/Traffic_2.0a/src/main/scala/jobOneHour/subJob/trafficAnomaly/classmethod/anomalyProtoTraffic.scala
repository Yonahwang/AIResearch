package jobOneHour.subJob.trafficAnomaly.classmethod

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Range
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018年6月26日 0026.
  */
class anomalyProtoTraffic(mysparksession: SparkSession, properties: Properties) extends corrFuction
  with Serializable {

  def anomalyProtoMain(targetipArr: Array[String], sourceDF: DataFrame,
                       happentimeH: String, protocol: String): DataFrame = {
    // 读取允许连续超过最大阈值的次数
    val maxNum = properties.getProperty("baseline.out.number").toInt
    // 定义一小时内的12个5min
    val minArr = Array("00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55")
    // 发生的一小时（12个5min）
    val happentimeM_arr: Array[String] = minArr.map(happentimeH + _)
    // 得到当前1h的相关协议12个实际流量值
    val (noNaN_netflow_nums, hour_traffic_dataArr) = get_actual_hourNetflow(targetipArr, happentimeM_arr, protocol)
    // 得到当前1h的相关协议12个流量基线值
    val hour_baseline_dataArr = get_baseline_hourNetflow(targetipArr, happentimeM_arr, protocol)
    // 定义异常相关信息dataframe的初始值为空
    var anomaly_eventDF: DataFrame = null
    try {
      // 必须保证网元ip不为null
      if (targetipArr.size > 1) {
        // 只有当前时刻Postgresql有数据，并且1h内的数据必须有maxNum条才能对比
        if (noNaN_netflow_nums >= maxNum) {
          // 转置矩阵
          val tras_traffic_data: Array[Array[Double]] = transposeDouble(hour_traffic_dataArr)
          val tras_baseline_data: Array[Array[Double]] = transposeDouble(hour_baseline_dataArr)

          // 对比实际流量和流量基线，得到异常值
          anomaly_eventDF = trafficVSbaseline(tras_traffic_data, tras_baseline_data,
            targetipArr, sourceDF, protocol, happentimeH, maxNum)

          if (anomaly_eventDF.count() > 0) {
            // 修改各个时刻异常点的值
            val edit_hour_current_trafficDF = editAnomalyIpValue(anomaly_eventDF, happentimeH, protocol)
            // 重新入库，更新替换之前发生异常时间的ip对应的value值
            updateAnomalyIpPostgresql(edit_hour_current_trafficDF, protocol)
          }
        } else {
          logger.error(happentimeH + "->" + protocol + "当前一小时的网元ip的数据大部分为空，不能正常处理各个ip对应的数据值，"
            + "不予进行基线对比；===》无流量异常的ip")
        }
      }
    } catch {
      case e: Exception => logger.error("Error：" + e.getMessage)
    } finally {}

    anomaly_eventDF
  }

  /**
    * 根据相关异常ip进行实际流量值的关联，用NaN替换异常ip的流量值
    *
    * @param anomaly_eventDF
    * @param happentimeH
    * @param protocol
    * @return
    */
  def editAnomalyIpValue(anomaly_eventDF: DataFrame, happentimeH: String, protocol: String) = {
    import mysparksession.implicits._
    import org.apache.spark.sql.functions._
    // 上报异常的ip及相关异常时间
    val anomalyDF: DataFrame = anomaly_eventDF.select("anomaly_ip", "anomaly_min")
      .withColumn("anomaly_min", explode(split($"anomaly_min", "[#]")))
      .groupBy("anomaly_min").agg(concat_ws("#", collect_set("anomaly_ip")))
      .withColumnRenamed("concat_ws(#, collect_set(anomaly_ip))", "anomaly_ips")

    // 1h的内所有ip对应的实际流量值（包括正常ip和异常ip）
    val hour_current_trafficDF: DataFrame = getHourCurrentTraffic(happentimeH, protocol)

    // 数据预处理，根据发生的异常时间作关联
    val process_trafficDF: DataFrame = hour_current_trafficDF.join(anomalyDF,
      hour_current_trafficDF("happentime") === anomalyDF("anomaly_min"), "inner")
      .select("happentime", "ips", "anomaly_ips", "all_data")

    // 修改异常ip对应的值
    val edit_hour_current_trafficDF: RDD[(String, String, String)] = process_trafficDF.rdd.map { line =>
      val happentime = line.getAs[String]("happentime")
      val ips: String = line.getAs[String]("ips")
      val anomaly_ips: String = line.getAs[String]("anomaly_ips")
      val all_data: String = line.getAs[String]("all_data")
      // 将异常ip的对应的value值替换为NaN
      val new_all_data = replaceAnomalyIpValue(ips, anomaly_ips, all_data)

      (happentime, ips, new_all_data)
    }

    edit_hour_current_trafficDF
  }

  /**
    * 将异常ip对应的value值替换为NaN
    *
    * @param ips
    * @param anomaly_ips
    * @param all_data
    * @return
    */
  def replaceAnomalyIpValue(ips: String, anomaly_ips: String, all_data: String): String = {
    val ipsArr: Array[String] = ips.split("#")
    val anomaly_ipsArr: Array[String] = anomaly_ips.split("#")
    val new_all_dataArr: Array[String] = all_data.split("#").clone()

    if (ipsArr.size == new_all_dataArr.size) {
      anomaly_ipsArr.foreach { anomaly_ip =>
        val index: Int = ipsArr.indexOf(anomaly_ip)
        // 如果index=-1，则说明这个数组不存在这个值
        if (index > 0) {
          // 将异常ip对应的value值替换为NaN
          new_all_dataArr.update(index, "NaN")
        }
      }
    }

    new_all_dataArr.mkString("#")
  }

  /** *
    * 更新异常ip的流量值，入库更新
    *
    * @param edit_hour_current_trafficDF
    * @param protocol
    */
  def updateAnomalyIpPostgresql(edit_hour_current_trafficDF: RDD[(String, String, String)], protocol: String) = {
    // (happentime, ips, new_all_data)
    // 设置postgresql配置参数
    val table = properties.getProperty("postgre.table.name.train")
    val conn_url = properties.getProperty("postgre.address")

    logger.error("异常ip对应的值更新替换，准备更新导入" + conn_url + "=>" + table)
    edit_hour_current_trafficDF.foreachPartition {
      part =>
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_url, properties.getProperty("postgre.user"),
          properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              val (happentime, ips, new_all_data) = line
              val sqlText =
                s"""UPDATE $table SET baselineresult= '$new_all_data'
                   | WHERE typeresult='$happentime' AND type='异常流量' AND identityresult='ip_data'
                   | AND standby03='$protocol' AND standby02='$ips'
             """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              prep.executeUpdate
              logger.error("Successed: " + happentime + "->" + protocol + " --此条记录更新成功")
            } catch {
              case e: Exception => logger.error("更新出错" + e.getMessage)
            } finally {}

        }
        conn.close
    }
  }

  /**
    * 得到当前1h的相关ip实际流量值，方便更新异常ip的流量值
    *
    * @param happentimeH
    * @param protocol
    * @return
    */
  def getHourCurrentTraffic(happentimeH: String, protocol: String): DataFrame = {
    val happen_start_time = happentimeH + "00"
    val happen_end_time = happentimeH + "55"
    val postgresqlUsr = properties.getProperty("postgre.user")
    val postgresqlPwd = properties.getProperty("postgre.password")
    val postgresqlUrl = properties.getProperty("postgre.address")
    val postgresqlTable = properties.getProperty("postgre.table.name.train")
    val dbtableSql =
      s"""
         |(SELECT temp.typeresult AS happentime, temp.standby02 AS ips, temp.baselineresult AS all_data
         | FROM $postgresqlTable AS temp
         | WHERE temp.typeresult>='$happen_start_time' AND temp.typeresult<='$happen_end_time'
         | AND temp.identityresult='ip_data' AND temp.type='异常流量' AND temp.standby03='$protocol'
         |) AS trafficdata""".stripMargin

    val options = Map("driver" -> "org.postgresql.Driver", "url" -> postgresqlUrl, "dbtable" -> dbtableSql,
      "user" -> postgresqlUsr, "password" -> postgresqlPwd)
    var hour_traffic_dataDF: DataFrame = null
    try {
      hour_traffic_dataDF = mysparksession.read
        .format("jdbc")
        .options(options)
        .load()
      if (hour_traffic_dataDF.count() > 0) {
        println("Sucess read:" + postgresqlTable + "->" + protocol + ",获取当前数据；时间段："
          + happen_start_time + "---" + happen_end_time)
      }
    } catch {
      case e: Exception => println("Error read:" + e.getMessage)
    } finally {}

    hour_traffic_dataDF
  }

  /**
    * 读取历史数据库表，每5min各个ip对应的协议字节数聚合值
    *
    * @param traffic_timeM
    * @param protocol
    * @param targetipArr
    * @return
    */
  def readPostgresqlHistoryData(traffic_timeM: String, protocol: String,
                                targetipArr: Array[String]): (Boolean, Array[Double]) = {
    val postgresqlUsr = properties.getProperty("postgre.user")
    val postgresqlPwd = properties.getProperty("postgre.password")
    val postgresqlUrl = properties.getProperty("postgre.address")
    val postgresqlTable = properties.getProperty("postgre.table.name.train")
    val dbtableSql =
      s"""
         |(SELECT temp.standby02 AS ips, temp.baselineresult AS all_data FROM $postgresqlTable AS temp
         | WHERE temp.standby03='$protocol' AND temp.typeresult='$traffic_timeM'
         | AND temp.identityresult='ip_data' AND temp.type='异常流量'
         |) AS trafficdata""".stripMargin
    val options = Map("driver" -> "org.postgresql.Driver", "url" -> postgresqlUrl, "dbtable" -> dbtableSql,
      "user" -> postgresqlUsr, "password" -> postgresqlPwd)


    // 如果此时刻的记录为0条，所有ip赋值为NaN
    var traffic_dataArr = getValueArr(targetipArr.size, Double.NaN)

    var noNaN_netflow_flag = false

    try {
      val traffic_dataDF = mysparksession.read
        .format("jdbc")
        .options(options)
        .load()
      if (traffic_dataDF.count() > 0) {
//        println("Sucess read:" + postgresqlTable + "->" + protocol + traffic_timeM + ",获取当前数据")
        // 此时刻的记录不为0, 得到各个ip当前5min的值
        traffic_dataArr = getAllIpData(traffic_dataDF, targetipArr)
        noNaN_netflow_flag = true
      } else {
//        println("Error read:" + postgresqlTable + "->" + protocol + traffic_timeM + "不存在此条记录")
      }
    } catch {
      case e: Exception => println("Error read:" + e.getMessage)
    } finally {
    }

    (noNaN_netflow_flag, traffic_dataArr)
  }

  /**
    * 得到当前1h内各个ip的相关协议字节数的实际值
    *
    * @param targetipArr
    * @param happentimeM_arr
    * @param protocol
    * @return
    */
  def get_actual_hourNetflow(targetipArr: Array[String], happentimeM_arr: Array[String],
                             protocol: String): (Int, Array[Array[Double]]) = {

    logger.error("1.获取当前1h内12个5min的实际流量, waiting>>>.................." + protocol)
    val hour_traffic_dataArr = new ArrayBuffer[Array[Double]]
    val ipNums = targetipArr.size
    // 定义一小时内的12条记录中为非NaN的个数
    var noNaN_netflow_nums = 0

    for (traffic_timeM <- happentimeM_arr) {
      // 实际流量的时间格式（yyyyMMddHHmm）
      val (noNaN_netflow_flag, traffic_dataArr) = readPostgresqlHistoryData(traffic_timeM, protocol, targetipArr)
      if (noNaN_netflow_flag) {
        noNaN_netflow_nums += 1
      }

      hour_traffic_dataArr += traffic_dataArr
    }

    (noNaN_netflow_nums, hour_traffic_dataArr.toArray)
  }

  /**
    * 得到当前1h内各个ip的相关协议字节数的基线值
    *
    * @param targetipArr
    * @param happentimeM_arr
    * @param protocol
    * @return
    */
  def get_baseline_hourNetflow(targetipArr: Array[String], happentimeM_arr: Array[String],
                               protocol: String): Array[Array[Double]] = {

    // 训练周期（默认为7天）
    val period = properties.getProperty("train.period.day").toInt
    val ipNums = targetipArr.size
    val hour_baseline_dataArr = new ArrayBuffer[Array[Double]]
    logger.error("2.获取当前1h内12个5min的流量基线, waiting>>>.................." + protocol)

    for (traffic_timeM <- happentimeM_arr) {
      val traintimeMArr: Array[String] = getTrainMinuteTime(traffic_timeM, period).sorted
      // 插入NaN记录的次数，初始化为0次
      var insertNaN_num = 0
      // 初步提取训练数据
      var dayHM_data = new ArrayBuffer[Array[Double]]
      for (trainMTime <- traintimeMArr) {
        // 取当前时刻的记录，只取1条，转换为double类型，（可能存在trainMTime这一时刻的记录不存在）
        // 历史流量的时间格式（yyyyMMddHHmm）

        val (noNaN_netflow_flag, traffic_dataArr) = readPostgresqlHistoryData(trainMTime, protocol, targetipArr)
        if (!noNaN_netflow_flag) {
          insertNaN_num += 1
        }

        dayHM_data += traffic_dataArr

      }
      //初始化所有ip当前时刻的流量基线
      var baseline_dataArr: Array[Double] = getValueArr(ipNums, Double.NaN)

      //---对Vector向量转置（即每个ip，对应一个数组向量），可能存在NaN---
      if (insertNaN_num < period) {
        // 通过转置，得到各个ip对应的训练集
        val tras_dayHM_data: Array[Array[Double]] = transpose(dayHM_data.toArray)
        //----- 数据预处理，获取所有ip当前时刻的流量基线
        baseline_dataArr = {
          for (i <- Range(0, ipNums)) yield {
            //          val ip = targetipArr(i)
            val ip_data: Array[Double] = tras_dayHM_data(i)
            //剔除ip对应数组中的NaN,方便计算3-sigma
            val no_array: Array[Double] = ip_data.filter({
              x: Double => !(x.isNaN)
            })
            if (no_array.size > 0) {
              // 将当前ip对应的历史数据进行3-&方法求出最大阈值，流量基线
              val ip_Maxconfidence: Double = getMaxConfidence(no_array)
              // 此时刻ip对应的最大阈值
              ip_Maxconfidence.formatted("%.2f").toDouble
            }
            else {
              //            logger.error(ip + "在该" + dayHM + "时刻不存在历史数据，设此时刻该ip的流量基线为NaN")
              // 将此时刻ip对应的最大阈值设置为NaN
              Double.NaN
            }
          }
        }.toArray
      } else {
        logger.error("所有ip在该" + traffic_timeM + "时刻不存在历史数据，设此时刻所有ip的流量基线都为空" + "->" + protocol)
      }
      hour_baseline_dataArr += baseline_dataArr
    }
    hour_baseline_dataArr.toArray
  }

  /**
    * 将矩阵进行转置
    *
    * @param m
    * @return
    */
  def transpose(m: Array[Array[Double]]): Array[Array[Double]] = {
    val transpose_Arr: Array[Array[Double]] = {
      for (c <- m(0).indices) yield {
        m.map(_ (c))
      }
    }.toArray

    transpose_Arr
  }

  /**
    * 得到数组中的置信区间最大值
    *
    * @param arr
    * @return
    */
  def getMaxConfidence(arr: Array[Double]): Double = {
    val size = arr.size
    var max_confidence: Double = 0.0
    if (size == 1) {
      // 如果历史数据只有一条
      max_confidence = 3 * arr(0)
    }
    else {
      val mean = arr.sum / size
      var sum = 0.0
      for (i <- arr) {
        sum += math.pow(i - mean, 2)
      }
      val stdev = math.sqrt(sum / size)
      max_confidence = mean + 3 * stdev
    }
    max_confidence
  }

  /**
    * 根据网元ip数组来提取dataframe中ip的相关数据
    *
    * @param traffic_dataDF
    * @param targetipArr
    * @return
    */
  def getAllIpData(traffic_dataDF: DataFrame, targetipArr: Array[String]): Array[Double] = {
    // 将ips和all_data一一映射
    val (ips, all_data) = traffic_dataDF.rdd.map {
      line =>
        (line.getAs[String]("ips").split("#"), line.getAs[String]("all_data").split("#"))
    }.take(1)(0)
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
    * 得到训练时间数组
    *
    * @param nowdayHM
    * @param period
    * @return
    */
  def getTrainMinuteTime(nowdayHM: String, period: Int): Array[String] = {

    var trainMTArr = new ArrayBuffer[String]()
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val nowtime = dateFormat.parse(nowdayHM)
    val cal = Calendar.getInstance()
    cal.setTime(nowtime)

    /*-------训练周期为7--------*/
    for (i <- Range(0, period)) {
      cal.add(Calendar.HOUR, -24)
      val current_time = dateFormat.format(cal.getTime)
      trainMTArr += current_time
    }
    trainMTArr.toArray
  }

  /**
    * 构造n个相同值的数组
    *
    * @param num
    * @param value
    * @return
    */
  def getValueArr(num: Int, value: Double): Array[Double] = {

    val valueArr = new ArrayBuffer[Double]()

    for (i <- Range(0, num)) {
      valueArr += value
    }
    valueArr.toArray
  }

  /**
    * 将格式为yyyyMMddHHmm变为yyyy-MM-dd HH:mm:00
    *
    * @param time
    * @return
    */
  def timeTotime(time: String) = {
    val year = time.substring(0, 4)
    val month = time.substring(4, 6)
    val day = time.substring(6, 8)
    val hour = time.substring(8, 10)
    val min = time.substring(10, 12)
    val newtime = year + "-" + month + "-" + day + " " + hour + ":" + min + ":00"

    newtime
  }

  /**
    * 转置矩阵(字符串类型)
    *
    * @param m
    * @return
    */
  def transposeDouble(m: Array[Array[Double]]): Array[Array[Double]] = {
    val transpose_Arr: Array[Array[Double]] = {
      for (c <- m(0).indices) yield {
        m.map(_ (c))
      }
    }.toArray

    transpose_Arr
  }

  /**
    * 实际流量和流量基线对比
    *
    * @param tras_traffic_data
    * @param tras_baseline_data
    * @param targetipArr
    * @param sourceDF
    * @param protocol
    * @param happened_timeH
    * @param maxNum
    * @return
    */
  def trafficVSbaseline(tras_traffic_data: Array[Array[Double]], tras_baseline_data: Array[Array[Double]],
                        targetipArr: Array[String], sourceDF: DataFrame, protocol: String,
                        happened_timeH: String, maxNum: Int): DataFrame = {

    // 所有ip的个数
    val ipNums = targetipArr.length

    // 定义异常流量
    var anomalyRDD: RDD[(String, String, String, String, String, String,
      String, String, String, String, String)] = null
    val anomalyArr = new ArrayBuffer[(String, String, String, String, String,
      String, String, String, String, String, String)]()

    // 定义sourceDF为null，标识符为true
    var sourceDF_null_flag = true
    if (sourceDF != null) {
      sourceDF_null_flag = false
      sourceDF.persist()
    }

    // -------对比traffic和baseline-----
    val min: Array[String] = Array("00", "05", "10", "15", "20", "25", "30", "35", "40", "45", "50", "55")
    // 对所有ip的实际流量和流量基线作对比
    for (i <- Range(0, ipNums)) {
      val evidenceArr = new ArrayBuffer[String]()
      val anomalyMinArr = new ArrayBuffer[String]()
      val anomalyValueArr = new ArrayBuffer[String]()
      val trafficArr: Array[Double] = tras_traffic_data(i)
      val baselineArr: Array[Double] = tras_baseline_data(i)

      // 统计此ip当前时刻实际流量traffic一小时内12个值非NaN的个数
      val traffic_noNaN_num: Int = trafficArr.count(!_.isNaN)

      // 将此ip当前时刻流量基线baseline一小时内12个值的NaN用平均值替换
      val no_NaN_baselineArr: Array[Double] = avgReplaceNan(baselineArr)

      // 只有满足1.当前时刻ip的实际traffic一小时内12个值中非空值大于maxNum，
      // -------2.流量基线非空；才进行比较
      if ((traffic_noNaN_num > maxNum) && (no_NaN_baselineArr != null)) {
        // 一小时的12个5min
        for (j <- Range(0, 12)) {
          if (trafficArr(j).isNaN) {
            evidenceArr += "NaN"
          } else if (trafficArr(j) <= no_NaN_baselineArr(j)) {
            evidenceArr += "NaN"
          } else {
            // 超过了流量基线
            val trafficStr: String = trafficArr(j).formatted("%.2f")
            val baselineStr: String = no_NaN_baselineArr(j).formatted("%.2f")
            val anomalytime_min: String = happened_timeH + min(j)
            val evidence = "该资产在" + timeTotime(anomalytime_min) + "之后的5分钟内的字节数为 " + trafficStr +
              ",超过了流量基线(字节数：" + baselineStr + ")"
            evidenceArr += evidence
            anomalyMinArr += anomalytime_min
            anomalyValueArr += trafficStr
          }
        }
      }

      val evidence_Arr: Array[String] = evidenceArr.toArray
      val anomalyMin_Arr: Array[String] = anomalyMinArr.toArray
      val anomalyValue_Arr: Array[String] = anomalyValueArr.toArray

      if (evidence_Arr.size > 0) {
        // 是否连续多次超过流量基线
        val flag = continueOutBase(evidence_Arr, maxNum)
        // 如果存在ip连续多次超过流量基线
        if (flag) {
          // 发生流量异常的ip
          val ip: String = targetipArr(i)

          // 发生流量异常的证据
          val evidencesStr = evidenceArr.filter {
            _ != "NaN"
          }.mkString("#")

          // 流量异常的溯源
          val sourcesStr: String =
            if (sourceDF_null_flag) {
              ""
            } else {
              sourceDF.filter(s"""srcip='$ip' OR dstip='$ip' AND protocol='$protocol'""").take(10).map {
                line =>
                  line.getAs[String]("source")
              }.mkString("#")
            }

          // 流量异常的实际值（因为实际值可能存在NaN）
          val trafficStr: String = trafficArr.map {
            line =>
              if (line.isNaN) {
                "0.0"
              } else {
                line.formatted("%.2f")
              }
          }.mkString("#")

          // 流量异常的基线值（之前已经用平均值替换了NaN）
          val baselineStr: String = no_NaN_baselineArr.map {
            line =>
              line.formatted("%.2f")
          }.mkString("#")

          //(新加) 流量异常的地理位置
          val positionStr: String =
            if (sourceDF_null_flag) {
              ""
            } else {
              sourceDF.filter(s"""srcip='$ip' AND protocol='$protocol'""").take(10).map {
                line =>
                  line.getAs[String]("position")
              }.mkString("#")
            }

          //(新加) 流量异常的是否含有tls加密流量
          val encryptStr: String =
            if (sourceDF_null_flag) {
              "非加密流量"
            } else {
              val encryptedRDD = sourceDF.filter(s"""srcip='$ip' OR dstip='$ip' AND protocol='$protocol'""").rdd.map {
                line => line.getAs[String]("proto7").toLowerCase()
              }.distinct().filter(_ == "tls")
              if (encryptedRDD.count() > 0) "加密流量" else "非加密流量"
            }

          //(新加) 流量异常的发生的异常时间（精确min）
          val anomalyMinStr: String = anomalyMinArr.mkString("#")

          //(新加) 流量异常各个异常时间的异常值（精确min）
          val anomalyValueStr: String = anomalyValueArr.mkString("#")


          anomalyArr += Tuple11(ip, happened_timeH, evidencesStr, sourcesStr,
            trafficStr, baselineStr, positionStr, encryptStr, anomalyMinStr, anomalyValueStr, protocol)
        }
      }

    }

    if (!sourceDF_null_flag) {
      sourceDF.unpersist()
    }

    anomalyRDD = mysparksession.sparkContext.makeRDD(anomalyArr.toArray)

    import mysparksession.implicits._

    anomalyRDD.toDF("anomaly_ip", "happen_time", "evidences", "sources", "traffics", "baselines", "positions",
      "encrypted", "anomaly_min", "anomaly_value", "protocol")
  }

  /**
    * 用数组的平均值替换NaN
    *
    * @param Arr
    * @return
    */
  def avgReplaceNan(Arr: Array[Double]): Array[Double] = {
    var newArr: Array[Double] = null
    val no_nanArr = Arr.filter(!_.isNaN)
    val no_nanLen = no_nanArr.size
    if (no_nanLen > 0) {
      val avg = no_nanArr.sum / no_nanLen
      newArr = Arr.map {
        line =>
          if (line.isNaN) {
            avg
          } else {
            line
          }
      }
    }
    newArr
  }


  /**
    * 统计1小时内是否存在有连续超过流量基线的ip
    *
    * @param ip_evidences
    * @param maxNum
    * @return
    */
  def continueOutBase(ip_evidences: Array[String], maxNum: Int): Boolean = {
    // 将ip_evidences数组中NaN字符标识为0，非NaN字符标识为1
    val ip_data: Array[Int] = ip_evidences.map {
      line =>
        if (line == "NaN") {
          0
        } else {
          1
        }
    }
    // 定义flag变量表示是否连续多次超过baseline
    var flag: Boolean = false
    // ------计算连续超过baseline的次数
    val consecutivecount: Array[(Int, Int)] = consecutiveCount(ip_data)
    for (line <- consecutivecount) {
      // 1表示超过baseline的标识
      if (line._1 == 1) {
        //若连续超过基线的次数大于maxNum
        if (line._2 >= maxNum) {
          flag = true
        }
      }
    }

    flag
  }

  /**
    * 将ls[0, 0, 0, 1, 1, 0, 1, 1, 0] 转换为[(0, 3), (1, 2), (0, 1), (1, 2), (0, 1)]
    *
    * @param ls
    * @return
    */
  def consecutiveCount(ls: Array[Int]) = {
    var tag: Int = ls(0)
    var count: Int = 0
    var consecutivecount = Array[(Int, Int)]()
    for (i <- ls) {
      if (i != tag) {
        consecutivecount = consecutivecount :+ (tag, count)
        count = 0
      }
      count = count + 1
      tag = i
    }
    //    将最后一次遍历完成后的结果保存
    consecutivecount = consecutivecount :+ (tag, count)
    consecutivecount
  }


}
