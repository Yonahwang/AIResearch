package jobOneDay.subJob.NetflowBotNet.NetflowBotNetClass

import java.io.Serializable
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import jobOneDay.subClass.LoggerSupport

/**
  * Created by TTyb on 2018/2/5.
  */
class NetflowBotNetJudge(properties: Properties, spark: SparkSession) extends LoggerSupport with Serializable {
  //判断是否是僵尸网络
  def judgeBotNet(correlateData: DataFrame): DataFrame = {
    /**
      * judgeHeart 判断心跳连接
      * judgeCount 统计IP个数
      * judgeTime 判断时间窗口
      * judgeBytesize 判断字节数
      * judgeLength 判断相应长度
      * 复杂度:judgeLength>judgeTime>judgeCount>judgeHeart>judgeBytesize
      **/
    getModelResult(judgeCount(judgeLength(judgeTime(judgeHeart(judgeBytesize(correlateData))))))
  }

  //判断字节是否小于阈值
  def judgeBytesize(correlateData: DataFrame): DataFrame = {
    logger.error("判断是否是小字节")
    val thresholdByte = properties.getProperty("thresholdByte").toInt
    val code = (upbytesize: String, downbytesize: String) => {
      if (upbytesize.toInt + downbytesize.toInt < thresholdByte) 1 else 0
    }
    val addCol = org.apache.spark.sql.functions.udf(code)
    val addColDataframe = correlateData.withColumn("condition", addCol(correlateData("upbytesize"), correlateData("downbytesize"))).filter("condition=1")
    addColDataframe.drop("condition")
  }

  //判断是否在同一个时间窗口
  def judgeTime(dataFrame: DataFrame): DataFrame = {
    logger.error("判断是否在同一个时间窗口")
    val timeDataFrame = dataFrame.groupBy("MaliciousDomain")
      .agg(concat_ws("#", collect_set("recordtime")))
      .withColumnRenamed("concat_ws(#, collect_set(recordtime))", "stringTime")

    val timeDF = dataFrame.join(timeDataFrame, Seq("MaliciousDomain"), "right_outer")
    val timeWindow = properties.getProperty("timeWindow").toInt
    val code = (stringTime: String, recordtime: String) => {
      var flag = "0"
      val arrayTime = stringTime.split("#").map(x => x.toLong)
      val minTime = arrayTime.min
      val maxTime = arrayTime.max
      val middleTime = ((maxTime - timeWindow) + minTime) / 2
      if (maxTime >= minTime + timeWindow * 3) {
        if (recordtime.toInt >= minTime && recordtime.toInt <= (minTime + timeWindow)) {
          flag = "1"
        } else if (recordtime.toInt >= middleTime && recordtime.toInt <= (middleTime + timeWindow)) {
          flag = "2"
        } else if (recordtime.toInt >= (maxTime - timeWindow) && recordtime.toInt <= maxTime) {
          flag = "3"
        }
      } else {
        flag = "0"
      }
      flag
    }
    val addCol = org.apache.spark.sql.functions.udf(code)

    val addColDataframe = timeDF.withColumn("judgeTime", addCol(timeDF("stringTime"), timeDF("recordtime")))
    val timeWindows = addColDataframe.groupBy("MaliciousDomain")
      .agg(concat_ws("#", collect_set("judgeTime")))
      .withColumnRenamed("concat_ws(#, collect_set(judgeTime))", "flag")
    val codeWindows = (flag: String) => {
      val counts = flag.split("#").length
      if (counts >= 3) 1 else 0
    }
    val addCols = org.apache.spark.sql.functions.udf(codeWindows)
    val addColsDataframe = timeWindows.withColumn("counts", addCols(timeWindows("flag"))).filter("counts=1").drop("counts", "flag")
    val windowsDF = timeDF.join(addColsDataframe, Seq("MaliciousDomain"), "right_outer").filter("flow_id is not null")
    windowsDF.distinct()
  }

  //判断IP是否发生过间歇性会话
  /**
    * 这里还需要加入间歇性会话的时间段
    *
    * @param dataFrame
    * @return
    */
  def judgeHeart(dataFrame: DataFrame): DataFrame = {
    logger.error("判断是否存在间歇性会话")
    val tm = new Date().getTime - 86400000
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tim = fm.format(new Date(tm))
    val tableName = properties.getProperty("hostAttacks") + "_" + tim + "/" + properties.getProperty("hostAttacks") + "_" + tim
    logger.error("获取模型结果数据" + tableName)
    //抓取数据下来
    val baseData = spark.esDF(tableName, "").where("modeltype='间歇会话连接'").select("ip", "abnormaltype","abnormalproto").withColumnRenamed("ip", "heartIP").distinct()
    val heartDF = dataFrame.join(baseData, dataFrame("srcip") === baseData("heartIP"), "right_outer").filter("flow_id is not null").drop("heartIP")
    heartDF
  }

  //判断返回内容的长度是否一样
  def judgeLength(dataFrame: DataFrame): DataFrame = {
    logger.error("判断返回内容的长度是否一样")
    import spark.implicits._
    val domainLength = dataFrame.groupBy("MaliciousDomain", "answers_data_length").count()
      .rdd.map {
      line =>
        (line.getAs[String]("MaliciousDomain"), line.getAs[String]("answers_data_length"), line.getAs[Long]("count"))
    }.keyBy(_._1).reduceByKey {
      (ele1, ele2) =>
        val result = if (ele1._3 > ele2._3)
          ele1
        else
          ele2
        result
    }.map(_._2).toDF("MaliciousDomain", "answers_data_length", "count").drop("count")

    val lengthDF = dataFrame.join(domainLength, Seq("MaliciousDomain", "answers_data_length"), "right_outer").filter("flow_id is not null")
    lengthDF.distinct()
  }

  //判断里面每个域名的IP个数是否大于3个
  def judgeCount(dataFrame: DataFrame): DataFrame = {
    logger.error("判断IP个数是否大于3个")
    val countDataFrame = dataFrame.groupBy("MaliciousDomain")
      .agg(concat_ws("#", collect_set("srcip")))
      .withColumnRenamed("concat_ws(#, collect_set(srcip))", "countString")

    val code = (countString: String) => {
      val countArray = countString.split("#").distinct
      if (countArray.length >= 3) 1 else 0
    }
    val addCol = org.apache.spark.sql.functions.udf(code)
    val addColDataframe = countDataFrame.withColumn("judgeCount", addCol(countDataFrame("countString"))).filter("judgeCount=1").drop("judgeCount", "countString")
    val countIPDF = dataFrame.join(addColDataframe, Seq("MaliciousDomain"), "right_outer").filter("flow_id is not null")
    countIPDF
  }

  //关联昨天的主机沦陷结果数据
  def getModelResult(dataFrame: DataFrame): DataFrame = {
    logger.error("关联昨天的主机沦陷结果")
    val modelResultIndex = properties.getProperty("es.data.index.occupy")
    //抓取时间范围
    val startTime = Timestamp.valueOf(getTime(-1)).getTime
    val endTime = Timestamp.valueOf(getTime(0)).getTime
    val query = "{\"query\":{\"range\":{\"time\":{\"gte\":" + startTime + ",\"lte\":" + endTime + "}}}}"
    //抓取数据下来
    val modelResul = spark.esDF(modelResultIndex, query).select("ip").distinct()
    val resultDF = dataFrame.join(modelResul, dataFrame("srcip") === modelResul("ip"),"left_outer")
    resultDF
  }

  //获取时间
  def getTime(day: Int): String = {
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newTime: String = new SimpleDateFormat("yyyy-MM-dd" + " 00:00:00").format(time)
    newTime
  }
}
