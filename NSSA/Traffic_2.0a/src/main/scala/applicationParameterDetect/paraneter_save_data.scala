package applicationParameterDetect

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties

import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.util.Try
import java.net.{URI, URLDecoder, URLEncoder}
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}


/**
  * Created by Administrator on 2018/6/7.
  */
object paraneter_save_data {
  //  @transient lazy val logger = Logger.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR)
  //显示的日志级别
  var directory = new File("..")
  PropertyConfigurator.configure(directory + "/conf/applicationBaseLinelog4j.properties")

  //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
  var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/application_parameter_baseline.properties")))
    .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/application_parameter_baseline.properties")))
  var properties: Properties = new Properties()
  properties.load(ipstream)

  //配置spark环境
  val spark = getSparkSession(properties)
  val sc: SparkContext = spark.sparkContext
  val sql: SQLContext = spark.sqlContext


  def main(args: Array[String]): Unit = {


    while(true) {

//      //读取数据
      val data = getDataFromHDFSByMerge()
//      //获取功能数据
      val filterdata = get_target_data(data)
//      filterdata.show(10, false)
      //保存数据
      sava_to_hdfs(filterdata)
      import java.util.Date
      println("complete time :"+ new Date())

      Thread.sleep(300000)  //5分钟
    }


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


  //从HDFS读取数据
  def getDataFromHDFSByMerge()={

    //获取hdfs文件
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(properties.getProperty("hdfs.netflow.path")), conf)
    val fileList: Array[FileStatus] = hdfs.listStatus(new Path(properties.getProperty("hdfs.netflow.path")))
    //获取hdfs目录文件(过滤最后一个文件)
    val file = fileList.map(_.getPath.toString).sorted
    val target_file = Try(file(file.length-2)).getOrElse(file.head)
    hdfs.close()
    //读取所有文件数据
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> target_file)
    //          data = sql.read.options(options).format("com.databricks.spark.csv").load()
    val data = spark.read.json(target_file)
    data

  }

  // 获取功能url
  def get_target_data(data:DataFrame) = {
    //分别从以下两个角度识别功能：
    //1、uri上的广义功能(仅获取http协议中，content-type=text/html的数据)
    //2、action动作的功能
    val result = data.filter{
      line =>
        val dstip = line.getAs[String]("dstip")
        val flag_host = dstip == "172.16.10.213"
        flag_host
    }
    result
  }


  def sava_to_hdfs(data: DataFrame) = {
    import java.util.Date
    val time = new Date().getTime
    val time_transfer = new SimpleDateFormat("yyyyMMddHHMM").format(time)
    val path = "hdfs://10.130.10.41:9000/spark/https_of_213/"+time_transfer
    val opt = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
    data.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(opt).save()


  }




}
