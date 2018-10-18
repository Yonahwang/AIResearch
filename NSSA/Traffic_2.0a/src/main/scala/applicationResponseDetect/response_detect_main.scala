package applicationResponseDetect

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.util.{Properties, UUID}

import applicationResponseDetect.subClass.LoggerSet
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.util.Try

/**
  * Created by wzd on 2018/7/27.
  */
object response_detect_main extends  Serializable with LoggerSet{
  //  @transient lazy val logger = Logger.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
  var directory = new File("..")
  PropertyConfigurator.configure(directory + "/conf/applicationBaseLinelog4j.properties")

  def main(args: Array[String]): Unit = {

    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/application_response_baseline.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/application_response_baseline.properties")))
    var properties: Properties = new Properties()
    properties.load(ipstream)

    //配置spark环境
    val spark = getSparkSession(properties)
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext

    //读取基线数据  //读取响应基线，此处改为读取postgre的基线结果
    val baseline = read_baseline(spark, properties.getProperty("response.baseline.local"))
    println("基线数据：")
    baseline.show(5)
    //读取待检测数据
    val data = getDataFromLocal(spark, properties.getProperty("data.local"))
    println("待检测的数据：")
    data.show(5)
    //获取满足功能uri的数据
    val data_filter = get_utility_url(data, baseline)
    //计算时间差
    val data_transfer_time = compute_time(data_filter, spark)
    //判断是否满足响应基线
    val data_baseline = detect(data_transfer_time, baseline, spark)
    //保存到本地，此处改为存储到ES
//    save_to_local(data_baseline, properties.getProperty("response.result.local"))

  }

  def getSparkSession(properties: Properties): SparkSession = {
    val sparkconf = new SparkConf()
      .setMaster(properties.getProperty("spark.master.url"))
      .setAppName(properties.getProperty("spark.app.name"))
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }

  def read_baseline(spark:SparkSession, path:String) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val data = spark.read.options(options).format("com.databricks.spark.csv").load()
    data
  }

  def getDataFromLocal(spark:SparkSession, path:String) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val data = spark.read.options(options).format("com.databricks.spark.csv").load()
    data
  }

  // 获取功能uri中的请求和响应时间
  def get_utility_url(data:DataFrame, baseline:DataFrame) = {
//    println("获取功能url>>>>>>>>>>>>>>>>>>")
    //业务功能uri
    val uri = baseline.select("app_uri").rdd.map{
      case Row(a) => a.toString
    }.collect()
    //过滤
    val result = data.filter{
      line =>
        val temp_url = line.getAs[String]("uri")
        val temp_uri = temp_url.split("\\?").head
        uri.contains(temp_uri)
    }
    result
  }

  def compute_time(data:DataFrame, spark:SparkSession) = {
    import spark.implicits._
    val time = data.rdd.map{
      x =>
        val srcip = x.getAs[String]("srcip")
        val dstip = x.getAs[String]("dstip")
        val temp_url = x.getAs[String]("uri")
        val temp_uri = temp_url.split("\\?").head
        val content_length = x.getAs[String]("content_length")
        val request_time = x.getAs[String]("request_date")
        val response_time = x.getAs[String]("response_date")
        val time_diff = response_time.toLong - request_time.toLong
        (srcip, dstip, temp_url, temp_uri, request_time, response_time, time_diff.toString, content_length)
    }.toDF("srcip", "dstip", "url", "uri", "request_date", "response_date", "time_diff", "content_length")
    time
  }


  def detect(data:DataFrame, baseline:DataFrame, spark:SparkSession) = {
    //业务功能uri
    import spark.implicits._
    val baseline_array = baseline.rdd.distinct().collect()
    val result = data.rdd.map{
      line =>
        //获取当前数据的字段信息
        val id: String = UUID.randomUUID().toString
        val srcip = line.getAs[String]("srcip")
        val dstip = line.getAs[String]("dstip")
        val url = line.getAs[String]("url")
        val uri = line.getAs[String]("uri")
        val request_time = line.getAs[String]("request_date")
        val response_time = line.getAs[String]("response_date")
        val time_diff = line.getAs[String]("time_diff")
        val content_length = line.getAs[String]("content_length")
        //获取时间基线
        val temp_bl_time = baseline_array.filter{ x => x.getAs[String]("app_uri") == uri && x.getAs[String]("response_type") == "time"}.head
        val bl_time = temp_bl_time.getAs[String]("response_baseline").toDouble
        //获取报文大小基线
        val temp_bl_size = baseline_array.filter{ x => x.getAs[String]("app_uri") == uri && x.getAs[String]("response_type") == "size"}.head
        val bl_size = temp_bl_size.getAs[String]("response_baseline").toDouble
        //检测时间是否异常
        val time_detect = if(time_diff.toDouble <= bl_time) "normal_time" else "abnormal_time"
        //检测报文大小是否异常
        val size_detect = if(content_length.toDouble <= bl_size) "normal_size" else "abnormal_size"
        val resul_type = "1"
        (id, srcip, dstip, url, uri, bl_time.toString, bl_size.toString, time_detect, size_detect, resul_type)
    }.toDF("flow_id", "srcip", "dstip", "request_url", "app_uri", "baseline_time", "baseline_size", "detect_time_result", "detect_size_result", "resul_type")
    println("result:")
    result.show(10)
    result
  }

  //保存到本地
  def save_to_local(data:DataFrame, path:String) = {
    val file = new File(path).exists()  //判断条件
    if(!file){  //不存在则新建文件
      new File(path).createNewFile()
    }
    //创建文件对象
    val fileW = new FileWriter(path, false)
    val header = List("srcip", "dstip", "url", "uri", "request_date", "response_date", "time_diff", "content_length", "baseline_time", "baseline_size", "detect_time_result", "detect_size_result").mkString("\t")
    fileW.write(header+"\n")
    //写入文件
    data.collect().foreach{
      line =>
        val srcip = line.getAs[String]("srcip")
        val dstip = line.getAs[String]("dstip")
        val url = line.getAs[String]("url")
        val uri = line.getAs[String]("uri")
        val request_time = line.getAs[String]("request_date")
        val response_time = line.getAs[String]("response_date")
        val time_diff = line.getAs[String]("time_diff")
        val content_length = line.getAs[String]("content_length")
        val baseline_time = line.getAs[String]("baseline_time")
        val baseline_size = line.getAs[String]("baseline_size")
        val detect_time_result = line.getAs[String]("detect_time_result")
        val detect_size_result = line.getAs[String]("detect_size_result")
        val l = List(srcip,dstip,url,uri,request_time,response_time,time_diff,content_length,baseline_time,baseline_size,detect_time_result,detect_size_result)
        val temp = l.mkString("\t") + "\n"
        fileW.write(temp)
    }
    fileW.close()
  }




}
