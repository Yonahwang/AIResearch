package applicationParameterDetect

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.net.URLDecoder
import java.util.Properties
import applicationParameterDetect.parameterMain.logger
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.util.Try

/**
  * Created by wzd on 2018/7/25.
  */
object parameter_detect_main {
  //  @transient lazy val logger = Logger.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
  var directory = new File("..")
  PropertyConfigurator.configure(directory + "/conf/applicationParameterBaseLinelog4j.properties")

  def main(args: Array[String]): Unit = {

    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/application_parameter_baseline.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/application_parameter_baseline.properties")))
    var properties: Properties = new Properties()
    properties.load(ipstream)

    //配置spark环境
    val spark = getSparkSession(properties)
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext

    //读取基线数据
    val baseline = read_baseline(spark, properties.getProperty("data.baseline.result"))
    baseline.show(10)
    //读取待检测数据
    val data = getDataFromLocal(spark, properties.getProperty("data.local"))
    data.show(5, false)
    //过滤数据，获取功能数据
    val filterdata = get_utility_url(data)
    //转换url编码
    val transfer_code_data = transfer_code(filterdata)
    //分割uri和参数
    val parameter_data = split_utility_parameter(transfer_code_data)
    //过滤uri，并关联所有参数的基线
    val parameter_data_filter_uri = filter_uri(parameter_data, baseline, spark)
    //将参数转换成自定义字符
    val parameter_data_filter_uri_transfer = parameter_transfer(parameter_data_filter_uri, spark)
    //检测参数合规性，分别检查参数是否在指定长度范围，字符类型是否在指定集合范围
    val parameter_detect = parameter_detect_process(parameter_data_filter_uri_transfer, spark)
    //保存到本地，此处需要修改为存储到ES
//    save_to_local(parameter_detect, properties.getProperty("data.parameter.result"))
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



  //转换url编码
  def transfer_code(data:DataFrame) = {
    logger.error("转换url编码>>>>>>>>>>>>>>>>>>")
    val addcol = org.apache.spark.sql.functions.udf((str: String) => {
      Try(URLDecoder.decode(str,"UTF-8")).getOrElse(str)
    })
    val result = data.withColumn("uri_code", addcol(data("uri")))
    result
  }



  // 获取功能url
  def get_utility_url(data:DataFrame) = {
    //分别从以下两个角度识别功能：
    //1、uri上的广义功能(仅获取http协议中，content-type=text/html的数据)
    //2、action动作的功能
    logger.error("获取功能url>>>>>>>>>>>>>>>>>>")
    val result = data.filter{
      line =>
        val host = line.getAs[String]("dstip")
        val flag_host = host == "172.16.10.213"
        val str1 = line.getAs[String]("request_urL")
        //        val flag1 = if(str1 == null) false else str1.contains(".action")
        val col = line.getAs[String]("content_type")
        val str2 = if(col == null) "" else col
        val flag2 = str2.contains("text/html")
        val flag3 = str2.contains("json")
        val flag4 = str2.contains("x-www-form-urlencoded")
        flag_host && (flag2 || flag3 || flag4)
      //        flag_host && flag1
    }
    result
  }


  //分割uri和参数
  def split_utility_parameter(data:DataFrame) = {
    logger.error("分割uri和参数>>>>>>>>>>>>>>>>>>")
    val addcol1 = org.apache.spark.sql.functions.udf((str: String) => {
      Try(str.split("\\?")(0)).getOrElse(str)
    })
    val addcol2 = org.apache.spark.sql.functions.udf((str: String) => {
      Try(str.split("\\?")(1)).getOrElse("")
    })
    val result1 = data.withColumn("uri_utility", addcol1(data("uri_code")))
    val result2 = result1.withColumn("parameter", addcol2(data("uri_code")))
    result2.show(10, false)
    result2
  }


  def filter_uri(data:DataFrame, baseline:DataFrame,spark:SparkSession) = {
//    val uri = baseline.select("uri").rdd.map{ x => x.getAs[String]("uri")}.collect()
//    val data_filter = data.filter{ x => uri.contains(x.getAs[String]("uri"))}
    val baseline_df = baseline.rdd.collect().map{
      line =>
        import spark.implicits._
        val uri = line.getAs[String]("app_uri")
//        println("uri:"+uri)
        val data_filter = data.filter{ x => x.getAs[String]("uri_utility") == uri}  //过滤业务功能中的uri
//        println("data_filter:")
//        data_filter.show(5)
        val data_filter_param = data_filter.rdd.map{
          x =>
            val para = x.getAs[String]("parameter")
            val para_list = para.split("&")
            val base_line_para_list = line.getAs[String]("app_param").split("&")
            val para_list_flag = para_list.filter{x => base_line_para_list.contains(x)}
            val flag = if(para_list_flag.length == base_line_para_list.length) true else false  //判断是否存在业务功能中的参数
            //若命中业务功能
            var extra_param_lsit_mks = ""
//            var extra_para_len = ""
//            var extra_para_type = ""
            if(flag){
              //找出业务功能外的参数
              val extra_param = para_list.diff(base_line_para_list)
              val extra_param_lsit = extra_param.map{
                extra_x =>
                  val key = extra_x.split("=")(0)
                  val value = extra_x.split("=")(1)
//                  val base_line_target = baseline.filter{x => (x.getAs[String]("uri") == uri) && (x.getAs[String]("parameter") == key)}
//                  val para_len = base_line_target.map{ x => x.getAs[String]("length")}.head
//                  val para_type = base_line_target.map{ x => x.getAs[String]("type")}.head
                  val para_len = line.getAs[String]("param_length")
                  val para_type = line.getAs[String]("param_type")
//                  println("extra param:"+List(extra_x, para_len, para_type).mkString("-"))
                  List(extra_x, para_len, para_type).mkString("&")  //额外的参数、参数长度、参数类型
              }
              extra_param_lsit_mks = extra_param_lsit.mkString("#") //额外参数#额外参数#...
//              println("extra_param_lsit_mks:"+extra_param_lsit_mks)
            }

            val flow_id = x.getAs[String]("flow_id")
            val srcip = x.getAs[String]("srcip")
            val dstip = x.getAs[String]("dstip")
            val host = x.getAs[String]("host")
            val protocol = x.getAs[String]("protocol")
            val request_date = x.getAs[String]("request_date")
            val request_urL = x.getAs[String]("request_urL")
            val response_date = x.getAs[String]("response_date")
            val request_uri = x.getAs[String]("uri").split("\\?").head

            (flow_id, srcip, dstip, host, protocol, request_date, response_date, request_urL, request_uri, para, extra_param_lsit_mks)
        }.toDF("flow_id", "srcip", "dstip", "host", "protocol", "request_date", "response_date", "request_urL", "request_uri", "para", "extra_param_lsit_mks")
        data_filter_param
    }

    val baseline_df_reduce = baseline_df.reduce{
      (x, y) =>
        (x.union(y))
    }
    println("合并后的df:")
    baseline_df_reduce.show(10, false)
    baseline_df_reduce
  }


  def parameter_transfer(data:DataFrame, spark: SparkSession) = {
    logger.error("计算业务每个业务功能的参数长度和取值类型>>>>>>>>>>>>>>>>>>")
    val pattern_upper_char = "[A-Z]".r  //大写字母字符
    val pattern_tower_char = "[a-z]".r  //小写字符字符
    val pattern_num = "[0-9]".r //数字字符
    val pattern_chinese = "[\u4e00-\u9fa5]".r //中文
    val pattern_punctuation = """[!"#$%'()*+,-./:;<=>?@[\\]^_`{|}~]""".r //标点字符
    import spark.implicits._
    val result = data.rdd.map{
      line =>
        val extra_param_lsit_mks = line.getAs[String]("extra_param_lsit_mks")
        val parameter_list = extra_param_lsit_mks.split("#")
        val new_parameter_list = parameter_list.map{
          para => //对于每个参数
            val extra_para = Try(para.split("&")(0)).getOrElse("")
            val para_len = Try(para.split("&")(1)).getOrElse("")
            val para_type = Try(para.split("&")(2)).getOrElse("")
            val extra_para_key = Try(extra_para.split("=").head).getOrElse("")
            val extra_para_value = Try(extra_para.split("=")(1)).getOrElse("")
            val new_extra_para_value = extra_para_value.map{
              chr =>
                var result = "#"
                if(!(pattern_upper_char.findFirstIn(chr.toString)).isEmpty){  //判断是否大写
                  result = "U"
                }
                if(!(pattern_tower_char.findFirstIn(chr.toString)).isEmpty){  //判断是否小写
                  result = "D"
                }
                if(!(pattern_num.findFirstIn(chr.toString)).isEmpty){  //判断是否数字
                  result = "N"
                }
                if(!(pattern_chinese.findFirstIn(chr.toString)).isEmpty){  //判断是否中文
                  result = "C"
                }
                if(!(pattern_punctuation.findFirstIn(chr.toString)).isEmpty){  //判断是否符号
                  result = "P"
                }
                result
            }.mkString("-")
            extra_para_key + "=" +new_extra_para_value + "&" + para_len + "&" + para_type
        }.mkString("#")
        val flow_id = line.getAs[String]("flow_id")
        val srcip = line.getAs[String]("srcip")
        val dstip = line.getAs[String]("dstip")
        val host = line.getAs[String]("host")
        val protocol = line.getAs[String]("protocol")
        val request_date = line.getAs[String]("request_date")
        val request_urL = line.getAs[String]("request_urL")
        val response_date = line.getAs[String]("response_date")
        val request_uri = line.getAs[String]("request_uri")
        val para = line.getAs[String]("para")
        (flow_id, srcip, dstip, host, protocol, request_date, response_date, request_urL, request_uri, para, extra_param_lsit_mks, new_parameter_list)
    }.toDF("flow_id", "srcip", "dstip", "host", "protocol", "request_date", "response_date", "request_urL", "request_uri", "para", "extra_param_lsit_mks", "new_parameter_list")
    result.show(5, false)
    result
  }


  def parameter_detect_process(data:DataFrame, spark:SparkSession) = {
    import spark.implicits._
    val result = data.rdd.map{
      line =>
        val extra_param_lsit_mks = line.getAs[String]("extra_param_lsit_mks")
        val parameter_list = extra_param_lsit_mks.split("#")
        val parameter_detect_result = parameter_list.map{
          sub_para => //IsDesktop=U-U-U-U-U-U-U-U-U-N&8-10&U-N
            val key = sub_para.split("&").head.split("=").head
            val value = sub_para.split("&").head.split("=")(1)
            val para_len_scope = sub_para.split("&")(1)
            val para_type_scope =sub_para.split("&")(2)
            //检测长度
            var para_len = value.split("-").length
            val para_len_scope_min = para_len_scope.split("-").head.toInt
            val para_len_scope_max = para_len_scope.split("-")(1).toInt
            val para_len_scope_new = (para_len_scope_min to para_len_scope_max).toList
            val abnormal_len = if(para_len_scope_new.contains(para_len)) "normal_len" else "abnormal_len"
            //检测参数类型
            val value_new = value.split("-").toSet.toList
            val para_type_scope_new = para_type_scope.split("-")
            val flag = value_new.diff(para_type_scope_new).length !=0
            val abnormal_type = if(flag) "abnormal_type" else "normal_type"
            //返回值
            val abnormal = key + "&" + abnormal_len + "&" + abnormal_type
            abnormal
        }.mkString("#")
        val flow_id = line.getAs[String]("flow_id")
        val srcip = line.getAs[String]("srcip")
        val dstip = line.getAs[String]("dstip")
        val host = line.getAs[String]("host")
        val protocol = line.getAs[String]("protocol")
        val request_date = line.getAs[String]("request_date")
        val request_urL = line.getAs[String]("request_urL")
        val response_date = line.getAs[String]("response_date")
        val request_uri = line.getAs[String]("request_uri")
        val para = line.getAs[String]("para")
        val new_parameter_list = line.getAs[String]("new_parameter_list")
        val app_utlity = request_uri + "?" + para
        (flow_id, srcip, dstip, host, protocol, request_date, response_date, request_urL, app_utlity, request_uri, para, extra_param_lsit_mks, parameter_detect_result)
    }.toDF("flow_id", "srcip", "dstip", "host", "protocol", "request_date", "response_date", "request_urL", "app_utlity", "request_uri", "extra_app_para", "extra_param_lsit_mks", "parameter_detect_result")
    result.show(5, false)
    result
  }


  def save_to_local(data:DataFrame, path:String) = {
    data.rdd.repartition(1).saveAsTextFile(path)
  }



}
