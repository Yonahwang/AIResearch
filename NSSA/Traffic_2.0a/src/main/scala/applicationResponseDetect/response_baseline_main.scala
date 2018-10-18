package applicationResponseDetect

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.sql.DriverManager
import java.util.{Date, Properties, UUID}

import applicationResponseDetect.subClass.LoggerSet
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.util.Try

/**
  * Created by wzd on 2018/7/27.
  */
object response_baseline_main extends  Serializable with LoggerSet{
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

    //读取基线数据  //读取业务功能参数基线，此处需要修改为读取postgre的基线结果
    val baseline = read_baseline(spark, properties.getProperty("data.baseline.result"))
    baseline.show(10)
    //读取待检测数据
    val data = getDataFromLocal(spark, properties.getProperty("data.local"))
    data.show(5)
    //获取满足功能uri的数据
    val data_filter = get_utility_url(data, baseline)
    //计算请求时间和响应时间的时间差
    val data_transfer_time = compute_time(data_filter, spark)
    //计算时间差的基线（3σ准则）
    val data_baseline = compute_time_baseline(data_transfer_time, spark)
    //计算报文大小基线（3σ准则）
    val data_protocol_size = compute_protocol_baseline(data_filter, spark)
    //合并结果
    val union_df = data_baseline.union(data_protocol_size)
    //规范化格式
    val result = scale(union_df, spark)
    //保存到本地
    save_to_local(result, properties.getProperty("response.baseline.local"))
//    saveToPostgre(result, properties)
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

  def read_baseline(spark:SparkSession ,path:String) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val data = spark.read.options(options).format("com.databricks.spark.csv").load()
    data
//    val table = properties.getProperty("find.table")
//    val select = s"(SELECT app_utility, app_uri, app_param, param_name, param_length, param_type FROM $table WHERE result_type =0 ) AS devip"
//    val result = spark.read
//      .format("jdbc")
//      .option("driver", "org.postgresql.Driver")
//      .option("url", properties.getProperty("postgre.address"))
//      .option("dbtable", select)
//      .option("user",properties.getProperty("postgre.user"))
//      .option("password",properties.getProperty("postgre.password"))
//      .load()
//    result.show(5, false)
//    result
  }

  def getDataFromLocal(spark:SparkSession, path:String) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    val data = spark.read.options(options).format("com.databricks.spark.csv").load()
    data
  }

  // 获取功能uri中的请求和响应时间
  def get_utility_url(data:DataFrame, baseline:DataFrame) = {
    logger.error("获取功能url>>>>>>>>>>>>>>>>>>")
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
        val temp_url = x.getAs[String]("uri")
        val temp_uri = temp_url.split("\\?").head
        val request_time = x.getAs[String]("request_date").toLong
        val response_time = x.getAs[String]("response_date").toLong
        val time_diff = response_time - request_time
        (temp_url, temp_uri, time_diff)
    }.toDF("url", "uri", "time_diff")
    time
  }

  def compute_protocol_baseline(data:DataFrame, spark:SparkSession) = {
    import spark.implicits._
    val data_filter = data.rdd.map{
      x =>
        val temp_url = x.getAs[String]("uri")
        val temp_uri = temp_url.split("\\?").head
        val content_length = x.getAs[String]("content_length").toDouble
        (temp_url, temp_uri, content_length)
    }.toDF("url", "uri", "content_length")

    //业务功能uri
    import spark.implicits._
    val uri = data_filter.select("uri").rdd.distinct().map{
      case Row(a) => a.toString
    }.collect()
    //计算3σ准则
    val temp_result = uri.map{
      each_uri =>
        val temp_data = data_filter.filter{
          line =>
            line.getAs[String]("uri") == each_uri
        }
//        println("temp_data")
//        temp_data.show(5)
        val content_length = temp_data.select("content_length").rdd.map{
          case Row(a) => a.toString.toDouble
        }
        val content_length_mean = content_length.mean()
        val content_length_std = content_length.stdev()
        val time_baseline = content_length_mean + 3 * content_length_std
        (each_uri, time_baseline.toString, "size")
    }
    val result = spark.sparkContext.parallelize(temp_result).toDF("uri", "baseline","type")
    println("size result:")
    result.show(5)
    result
  }

  def compute_time_baseline(data:DataFrame, spark:SparkSession) = {
    //业务功能uri
    import spark.implicits._
    val uri = data.select("uri").rdd.distinct().map{
      case Row(a) => a.toString
    }.collect()
    //计算3σ准则
    val temp_result = uri.map{
      each_uri =>
        val temp_data = data.filter{
          line =>
            line.getAs[String]("uri") == each_uri
        }
        //获取时间差的数组
        val time_list = temp_data.select("time_diff").rdd.map{
          case Row(a) => a.toString.toDouble
        }
        val time_list_mean = time_list.mean()
        val time_list_std = time_list.stdev()
        val time_baseline = time_list_mean + 3 * time_list_std
        (each_uri, time_baseline.toString, "time")
    }//
    val result = spark.sparkContext.parallelize(temp_result).toDF("uri", "baseline","type")
    println("time result:")
    result.show(5)
    result
  }


  def scale(dataFrame: DataFrame, spark: SparkSession) = {
    import spark.implicits._
    val time = new Date().getTime.toString
    val result = dataFrame.rdd.map{
      line =>
        val id: String = UUID.randomUUID().toString
        val uri = line.getAs[String]("uri")
        val baseline = line.getAs[String]("baseline")
        val r_type = line.getAs[String]("type")
        val result_type = "1"
        (id, uri, baseline, r_type, time, result_type)
    }.toDF("id", "app_uri", "response_baseline", "response_type", "time", "result_type")
    result.show(10, false)
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
//    val header = List("uri", "baseline", "type").mkString("\t")
    val header = List("id", "app_uri", "response_baseline", "response_type", "time", "result_type").mkString("\t")
    fileW.write(header+"\n")
    //写入文件
    data.collect().foreach{
      line =>
        val id = line.getAs[String]("id")
        val app_uri = line.getAs[String]("app_uri")
        val response_baseline = line.getAs[String]("response_baseline")
        val response_type = line.getAs[String]("response_type")
        val time = line.getAs[String]("time")
        val result_type = line.getAs[String]("result_type")
        val temp = List(id,app_uri,response_baseline,response_type,time,result_type).mkString("\t") + "\n"
        fileW.write(temp)
    }
    fileW.close()
  }


  def saveToPostgre(data: DataFrame ,properties: Properties) = {
    val table = properties.getProperty("application.baseline.table")
    println("开始导入POSTGRESQL>>>")
    data.foreachPartition {
      part =>
        val conn_str = properties.getProperty("postgre.address")
        //        val conn_str = "jdbc:postgresql://172.16.1.108:5432/NXSOC5"
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              //"id", "app_uri", "response_baseline", "response_type", "time", "result_type"
              val sqlText =
                s"""INSERT INTO $table
                   |(id, app_uri, response_baseline,
                   |  response_type, time, result_type)
                   |VALUES (?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              prep.setString(1, line.getAs[String]("id"))
              prep.setString(2, line.getAs[String]("app_uri"))
              prep.setString(3, line.getAs[String]("response_baseline"))
              prep.setString(4, line.getAs[String]("response_type"))
              prep.setString(5, line.getAs[String]("time"))
              prep.setString(6, line.getAs[String]("result_type"))
              prep.executeUpdate
            } catch {
              case e: Exception => println("导入出错" + e.getMessage)
            }
            finally {}
        }
        conn.close
    }
  }


}
