package applicationParameterDetect

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.util.{Date, Properties, UUID}

import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import scala.util.Try
import subClass.{LoggerSet, readClass}

import scala.util.matching.Regex
import java.net.URLDecoder
import java.net.URLEncoder
import java.sql.{DriverManager, Timestamp}

import org.apache.spark.rdd.RDD


/**
  * Created by wzd on 2018/5/7.
  */


object parameterMain extends LoggerSet{

  //  @transient lazy val logger = Logger.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
  var directory = new File("..")
  PropertyConfigurator.configure(directory + "/conf/applicationBaseLinelog4j.properties")

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

    //读取数据
    val readDataClass = new readClass(spark, sql, properties)
//    val data = readDataClass.getDataFromHDFSByMerge()
    val data = readDataClass.getDataFromLocal(spark)
    //过滤数据，获取功能数据
    val filterdata = get_utility_url(data)
    //转换url编码
    val transfer_code_data = transfer_code(filterdata)
    //分割uri和参数
    val parameter_data = split_utility_parameter(transfer_code_data)
    //寻找业务功能
    val app_utility = find_app_utility(parameter_data)
    //返回业务功能、请求key-value的map: Map(功能->Map(key1->List(value), key2->List(value)))
    val parameter_pro = parameter_process(parameter_data, app_utility)
    //对每个业务功能的key-value对进行字符转换
    val parameter_stat = parameter_transfer(parameter_pro)
    //计算参数基线,分别计算每个业务功能的参数长度范围、字符类型，如
    //  /signalr/negotiate?clientProtocol=1.5&PageId=LayoutMessages&connectionData=[{"name":"chathub"}]&_=1528191898732 -> Map(IsDesktop -> (5-8,N-u))
    val parameter_result = parameter_baseline(parameter_stat)
    //规范化基线格式，返回List（业务功能、业务功能的uri字符、业务功能的参数字符、参数名称、参数长度范围、参数类型）
    val parameter_result_scale = parameter_scala(parameter_result, spark)
    //保存到本地，此处需要修改到存储到postgre
    save_to_local(parameter_result_scale, properties.getProperty("data.baseline.result"))
//    saveToPostgre(parameter_result_scale, properties)
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
//    val test = data.rdd.foreach{
//      line =>
//        val target = line.getAs[String]("uri")
//        val target_split = target.split("\\?")
//        val target_key_value = target_split(1)
//    }
//    test
    logger.error("分割uri和参数>>>>>>>>>>>>>>>>>>")
    val addcol1 = org.apache.spark.sql.functions.udf((str: String) => {
      Try(str.split("\\?")(0)).getOrElse(str)
    })
    val addcol2 = org.apache.spark.sql.functions.udf((str: String) => {
      Try(str.split("\\?")(1)).getOrElse("")
    })
    val result1 = data.withColumn("uri_utility", addcol1(data("uri_code")))
    val result2 = result1.withColumn("parameter", addcol2(data("uri_code")))
//    result2.show(10, false)
    result2
  }


  def find_app_utility(data:DataFrame) =  {
    logger.error("寻找业务功能>>>>>>>>>>>>>>>>>>")
    //找出被超过3个人访问的uri
    val temp_data = data.select("srcip","uri_utility").distinct()
    val temp_utility = temp_data.select("uri_utility").rdd.map(x => (x.getAs[String]("uri_utility"), 1))
    val group_utility = temp_utility.reduceByKey(_ + _) //统计访问过该uri的ip个数
    val filter_utility = group_utility.filter{x => x._2 >= 3}
    val utility_uri = filter_utility.map(x => x._1).collect()

    //针对每个uri分别计算业务功能
    val app_utility = utility_uri.map{
      sub_uri =>
        val temp_data = data.filter{x => x.getAs[String]("uri_utility") == sub_uri}
        val temp_data_param_list: RDD[List[String]] = temp_data.select("parameter").rdd.map{ x => x.getAs[String]("parameter").split("&").toList}
        val temp_data_param_list_reduce = temp_data_param_list.reduce{
          (x, y) =>
            x.intersect(y)
        }
        val temp_data_param_list_reduce_ms = temp_data_param_list_reduce.mkString("&")
        val app_utility = sub_uri + "?" + temp_data_param_list_reduce_ms  //组合业务功能
        app_utility
    }
    app_utility
  }




  def parameter_process(data:DataFrame, app_utility:Array[String])={
//    val utility = data.select("uri_utility").rdd.distinct().map{ x => x(0).toString}.collect().toList
//    println("uri_utility:"+utility)
    logger.error("计算业务每个业务功能对应的参数：key-values对>>>>>>>>>>>>>>>>>>")
    //获取业务功能、参数、取值的Map(utility->Map(key->List(value)))
    val parameter_dict: Map[String, Map[String, List[String]]] = app_utility.map{
      line =>

        //业务功能上的参数
        val utility_param_list = line.split("\\?").tail
        val utility_param_flag = if(utility_param_list.length != 0) utility_param_list.head else ""
        val utility_param = utility_param_flag.split("&")
        //业务功能上的uri
        val utility_uri = line.split("\\?").head
        //过滤功能uri的数据
        val temp_data = data.filter{ x => x.getAs[String]("uri_utility") == utility_uri}
        //获取该uri下的参数
        val para_list: List[String] = temp_data.select("parameter").rdd.distinct().map{ x => x.getAs[String]("parameter")}.collect().toList
        //过滤业务功能上的参数
        val para_list_filter = para_list.map{ x => x.split("&").diff(utility_param)}
        //分割参数：List(List(List(key1, value),List(key2, value)))
        val para_split = para_list_filter.map{
          x1 =>
            val list_split_split = x1.map{
              x2 =>
                x2.split("=").toList
            }
            list_split_split
        }
        //获取key的所有种类
        val key_set = para_split.flatMap{
          x1 =>
            x1.map{
              x2 =>
                x2(0)
            }
        }.distinct
        //获取每个key对应的value数组,返回Map(key->list(value))
        val key_set_list = key_set.map{
          key =>
            val temp_key_list = para_split.flatMap{
              x1 =>
                x1.map{
                  x2 =>
                    if(x2(0) == key) Try(x2(1)).getOrElse("no_data") else "placeholder"
                }
            }
            val temp_key_list_filter = temp_key_list.filter{ x => x != "no_data" && x != "placeholder" }
            (key, temp_key_list_filter)
        }.toMap
        (line, key_set_list)
    }.toMap
    println("parameter_dict:"+parameter_dict)
    parameter_dict
  }

  //判断每个业务功能的key-value对的取值范围
  def parameter_transfer(parameter_pro: Map[String, Map[String, List[String]]]) = {
    logger.error("计算业务每个业务功能的参数长度和取值类型>>>>>>>>>>>>>>>>>>")
    val pattern_upper_char = "[A-Z]".r  //大写字母字符
    val pattern_tower_char = "[a-z]".r  //小写字符字符
    val pattern_num = "[0-9]".r //数字字符
    val pattern_chinese = "[\u4e00-\u9fa5]".r //中文
    val pattern_punctuation = """[!"#$%'()*+,-./:;<=>?@[\\]^_`{|}~]""".r //标点字符

    val result = parameter_pro.keySet.map{
      key =>  //按业务功能
        val sub_key_set = parameter_pro(key).keySet //业务功能的key列表
        val sub_key_set_map = sub_key_set.map{
          sub_key =>  //每个key
            val key_list = parameter_pro(key)(sub_key)  //每个key对应的参数列表
            val key_value_list = key_list.map{
              parameter => //每个参数值value
                val temp_list = parameter.map{
                  ele =>  //每个元素
                    var result = "#"
                    if(!(pattern_upper_char.findFirstIn(ele.toString)).isEmpty){  //判断是否大写
                      result = "U"
                    }
                    if(!(pattern_tower_char.findFirstIn(ele.toString)).isEmpty){  //判断是否小写
                      result = "D"
                    }
                    if(!(pattern_num.findFirstIn(ele.toString)).isEmpty){  //判断是否数字
                      result = "N"
                    }
                    if(!(pattern_chinese.findFirstIn(ele.toString)).isEmpty){  //判断是否中文
                      result = "C"
                    }
                    if(!(pattern_punctuation.findFirstIn(ele.toString)).isEmpty){  //判断是否符号
                      result = "P"
                    }
                    result
                }
                temp_list.toList
            }
            (sub_key, key_value_list)
        }.toMap
        (key, sub_key_set_map)
    }.toMap
    println("parameter dict replace:"+result)
    result
  }

  //计算参数基线
  def parameter_baseline(parameter_stat: Map[String, Map[String, List[List[String]]]])={
    logger.error("计算参数基线>>>>>>>>>>>>>>>>>>")
    val result: Map[String, Map[String, (String, String)]] = parameter_stat.map{
      utility =>  //每个业务功能
        val key_map = utility._2.map{
          key_value =>  //每个key-value对
            val value_length = key_value._2.map{ value => value.length}   //每个value的长度
            val value_length_result = Try(value_length.min.toString + "-" + value_length.max.toString).getOrElse("0-0")   //保留最小最大的value值
            val key_unique = key_value._2.flatMap{ value => value }.toSet.mkString("-")   //value的类型种类
            (key_value._1, (value_length_result, key_unique))   //(key, (value长度, value字符种类))
        }
        (utility._1, key_map)   //(业务功能, (key, (value长度, value字符种类)))
    }
    println("parameter base line:"+result)
  //过滤没有参数的业务功能
  val filter_result = result.filter{ x => x._2.keySet.toList.length != 0}
    println("parameter base line filter:"+filter_result)
    logger.error("业务功能数："+result.keySet.toList.length)
    logger.error("业务功能基线数："+filter_result.keySet.toList.length)
    filter_result
  }

  //规范化基线格式
  def parameter_scala(parameter_result: Map[String, Map[String, (String, String)]], spark: SparkSession) = {

    val time = new Date().getTime
    val result = parameter_result.flatMap{
      utility =>
        val scale = utility._2.map{
          param =>
            val id: String = UUID.randomUUID().toString
            val uri: String = utility._1.split("\\?").head
            val temp_param = utility._1.split("\\?").tail
            val param_flag: String = if(temp_param.length != 0) temp_param.head else ""
//            val param_list = param_flag.split("&")
            val result_type: String = "0"
            List(id, utility._1.toString, uri, param_flag, param._1.toString, param._2._1.toString, param._2._2.toString, time.toString, result_type)//id, 业务功能、业务功能的uri字符、业务功能的参数字符、参数名称、参数长度范围、参数类型、时间、结果类型
        }.toList
        scale
    }.toList
//    import spark.implicits._
//    val result_df = spark.sparkContext.parallelize(result).toDF("id","app_utility","app_uri","app_param","param_name","param_length","param_type","time","result_type")
//    result_df.show(10, false)
//    result_df
    result
  }

  //保存到本地
  def save_to_local(parameter_result_scale: List[List[String]], path:String) = {
    val file = new File(path).exists()  //判断条件
    if(!file){  //不存在则新建文件
      new File(path).createNewFile()
    }
    //创建文件对象
    val fileW = new FileWriter(path, false)
//    val header = List("utility","uri","param","parameter","length","type").mkString("\t")
    val header = List("id","app_utility","app_uri","app_param","param_name","param_length","param_type","time","result_type").mkString("\t")
    fileW.write(header+"\n")
    //写入文件
    for(line <- parameter_result_scale){
      val temp = line.mkString("\t") + "\n"
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
              //"id","app_utility","app_uri","app_param","param_name","param_length","param_type","time","result_type"
              val sqlText =
                s"""INSERT INTO $table
                   |(id, app_utility, app_uri,
                   |  app_param, param_name, param_length,
                   |  param_type, response_baseline, response_type,
                   |  time, result_type)
                   |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              prep.setString(1, line.getAs[String]("id"))
              prep.setString(2, line.getAs[String]("app_utility"))
              prep.setString(3, line.getAs[String]("app_uri"))
              prep.setString(4, line.getAs[String]("app_param"))
              prep.setString(5, line.getAs[String]("param_name"))
              prep.setString(6, line.getAs[String]("param_length"))
              prep.setString(7, line.getAs[String]("param_type"))
              prep.setString(8, "")
              prep.setString(9, "")
              prep.setString(10, line.getAs[String]("time"))
              prep.setString(11, line.getAs[String]("result_type"))
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
