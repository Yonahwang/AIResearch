package AssetStatistics.SubClass

import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Yiutto on 2018年7月16日 0016.
  */
trait funcMethod extends toLogger {
  def readCsv1(sparkSession: SparkSession, path: String): DataFrame = {
    val options = Map("header" -> "true", "path" -> path, "delimiter" -> "\t")
    var csvDF: DataFrame = null
    try {
      csvDF = sparkSession.read
        .options(options)
        .format("com.databricks.spark.csv")
        .load()
      //      logger.error("Sucess read:" + path)
    } catch {
      case e: Exception => logger.error("Error read:" + e.getMessage)
    } finally {}

    csvDF
  }

  /**
    * 读取HDFS路径上的所有文件名
    *
    * @param hdfsPath
    * @return
    */
  def getHDFSFileName(hdfsPath: String): Array[String] = {
    logger.error("开始读HDFS文件夹上的所有文件名>>>")
    import java.net.URI
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

    var nameArr = Array[String]()

    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(hdfsPath), conf)
    try {
      val fs = hdfs.listStatus(new Path(hdfsPath))
      nameArr = FileUtil.stat2Paths(fs).map(_.getName).sorted
    } catch {
      case e: Exception => logger.error("出错：" + e.getMessage)
    } finally {
      hdfs.close()
    }

    nameArr
  }

  //合并dataframe函数
  def mergeDataFrame(dataFrameList: List[DataFrame]): DataFrame = {
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val unionData = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    unionData
  }

  def generateID(): String = {
    val row1 = ((math.random * 9 + 1) * 100000).toInt.toString
    val row2 = (new Date().getTime / 1000).toString
    row1 + row2
  }


  def getNowTime(): Long = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      .parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date))
      .getTime
    date
  }

  /**
    * 转置矩阵(double类型)
    * Array(Array(1.0, 2.0, 3.0), Array(4.0, 5.0, 6.0)) -> Array(Array(1.0, 4.0), Array(2.0, 5.0), Array(3.0, 6.0))
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
    * （因为用的是离线数据，所以计划00：30开始运行，才能保证取得到昨天的所有数据)，格式为yyyyMMdd
    * start_day（即训练开始时间），end_day（即训练结束时间），时间间隔为period天
    *
    * @param happen_day
    * @param period
    * @return
    */
  def getTrainday(happen_day: String, period: Int): (String, String) = {

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val happen_date = dateFormat.parse(happen_day)
    val cal = Calendar.getInstance()
    cal.setTime(happen_date)

    // (detect day的前一天作为train end day)
    cal.add(Calendar.HOUR, -24)
    val end_day: String = dateFormat.format(cal.getTime)

    // (detect day的前六天作为train start day)
    cal.add(Calendar.HOUR, -24 * (period - 1))
    val start_day: String = dateFormat.format(cal.getTime)

    (start_day, end_day)
  }

  /**
    * 主要是读取历史表中的('待检测闲置资产' 或 '待检测退网资产')
    * (ip2, happen_day2)
    *
    * @param mySparkSession
    * @param properties
    * @param asset_type ('待检测闲置资产' 或 '待检测退网资产')
    * @return
    */
  def readPostgresqlUndetecedAssetIP(mySparkSession: SparkSession, properties: Properties, asset_type: String): DataFrame = {
    val postgresqlUsr = properties.getProperty("postgre.user")
    val postgresqlPwd = properties.getProperty("postgre.password")
    val postgresqlUrl = properties.getProperty("postgre.address")
    val postgresqlTable = properties.getProperty("postgre.table.name.train")

    val querySql =
      s"""
         |(SELECT temp.ip AS ip2, temp.typeresult AS happen_day2 FROM $postgresqlTable AS temp
         | WHERE temp.identityresult='history_asset' AND temp.type='$asset_type'
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

  /**
    * 对比当前检测的ip1和历史表的ip2，如果ip1==ip2，happen_day1-happen_day2>=day_threshold,则上报该ip
    *
    * @param data1         当前检测的DataFrame("ip1", "happen_day1")
    * @param data2         历史表的DataFrame("ip2", "happen_day2")
    * @param day_threshold （30->退网资产、3->闲置资产）
    * @param flag          (5->退网资产、4->闲置资产)
    * @return
    */
  def getIdleOrOutIP(data1: DataFrame, data2: DataFrame, day_threshold: Int, flag: Int): RDD[(String, Int)] = {
    // data1("ip1", "happen_day1")
    // data2("ip2", "happen_day2")

    val dataDF: DataFrame = data1.join(data2, data1("ip1") === data2("ip2"))
    val idle_rdd: RDD[(String, Int)] = dataDF.rdd.map { line =>
      val ip = line.getAs[String]("ip1")
      val happen_day1 = line.getAs[String]("happen_day1")
      val happen_day2 = line.getAs[String]("happen_day2")
      val diff_day: Long = getDifferenceDay(happen_day1, happen_day2)
      var idle_flag: Int = 0
      if (diff_day >= day_threshold) {
        // flag表示(5->退网资产、4->闲置资产)
        idle_flag = flag
      }
      (ip, idle_flag)
    }.filter(f => f._2 == flag)

    idle_rdd
  }

  /**
    * 计算day1和day2的相差天数
    *
    * @param day1 格式为（yyyymmdd）
    * @param day2 格式为（yyyymmdd）
    */
  def getDifferenceDay(day1: String, day2: String): Long = {
    import java.text.SimpleDateFormat
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val date1 = dateFormat.parse(day1).getTime
    val date2 = dateFormat.parse(day2).getTime
    val day = (date1 - date2) / 24 / 3600 / 1000

    day
  }


  /**
    * 主要是将('待检测闲置资产' 或 '待检测退网资产')入库
    * 主要是更新插入
    *
    * @param mySparkSession
    * @param data  DataFrame的字段为（"ip1","happen_day1"）
    * @param properties
    * @param asset_type ('待检测闲置资产' 或 '待检测退网资产')
    * @return
    */
  def updatePostgresqlUndetecedAssetIP(mySparkSession: SparkSession, data: DataFrame,
                                       properties: Properties, asset_type: String) = {
    // (ip, happen_day)
    // 设置postgresql配置参数
    val table = properties.getProperty("postgre.table.name.train")
    val conn_url = properties.getProperty("postgre.address")

    logger.error("异常ip对应的值更新替换，准备更新导入" + conn_url + "=>" + table)
    data.foreachPartition {
      part =>
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_url, properties.getProperty("postgre.user"),
          properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              // 先更新
              val ip = line.getAs[String]("ip1")
              val happen_day = line.getAs[String]("happen_day1")
              val update_sqlText =
                s"""
                   | UPDATE $table SET typeresult=?, standby01=?
                   | WHERE identityresult='history_asset' AND type='$asset_type' AND ip='$ip'
                  """.stripMargin

              val update_prep = conn.prepareStatement(update_sqlText)
              update_prep.setString(1, happen_day)
              update_prep.setString(2, getNowTime().toString)
              val update_flag: Int = update_prep.executeUpdate

              // 如果这条记录不存在，则插入
              if (update_flag == 0) {
                val insert_sqlText =
                  s"""
                     |INSERT INTO $table (id, ip, typeresult, identityresult, type, standby01)
                     |VALUES (?,?,?,?,?,?)
                   """.stripMargin
                val insert_prep = conn.prepareStatement(insert_sqlText)
                insert_prep.setString(1, generateID())
                insert_prep.setString(2, ip)
                // 发生时间（yyyymmdd）
                insert_prep.setString(3, happen_day)
                insert_prep.setString(4, "history_asset")
                insert_prep.setString(5, asset_type)
                insert_prep.setString(6, getNowTime().toString)
              }
            } catch {
              case e: Exception => logger.error("出错" + e.getMessage)
            } finally {}

        }
        conn.close
    }
  }


  /**
    * 将（变化资产、退网资产、闲置资产）入库
    *
    * @param dataRDD
    * @param happen_day
    * @param properties
    */
  def assetToPostgresql(dataRDD: RDD[(String, Int)], happen_day: String, properties: Properties) = {
    // 设置postgresql配置参数
    val table = properties.getProperty("find.table")
    val conn_url = properties.getProperty("postgre.address")

    logger.error("资产统计（变化、闲置、退网）的ip，准备导入" + conn_url + "=>" + table)
    val rdd_counts: Long = dataRDD.count()
    if (rdd_counts > 0) {
      dataRDD.foreachPartition {
        part =>
          Class.forName("org.postgresql.Driver").newInstance
          val conn = DriverManager.getConnection(conn_url, properties.getProperty("postgre.user"),
            properties.getProperty("postgre.password"))
          part.foreach {
            line =>
              try {
                val ip = line._1
                val flg = line._2
                val sqlText =
                  s"""INSERT INTO $table (recordid, ip, storagetime, flg)
                     |VALUES (?, ?, ?, ?) """.stripMargin
                val prep = conn.prepareStatement(sqlText)
                prep.setString(1, generateID())
                prep.setString(2, ip)
                prep.setTimestamp(3, new Timestamp(getNowTime))
                prep.setInt(4, flg)
                prep.executeUpdate
              } catch {
                case e: Exception => logger.error("出错" + e.getMessage)
              } finally {}

          }
          conn.close
      }
      logger.error("Successed: " + happen_day + "->" + rdd_counts + "条记录入库成功")
    } else {
      logger.error("无相应资产的ip，不需要入库")
    }
  }
}
