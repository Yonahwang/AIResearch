package jobOneHour

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import jobOneHour.subClass.{LoggerSupport, methodReadData}
import jobOneHour.subJob.Heartbeat.heartbeat_autocorrelation
import jobOneHour.subJob.HostAttacks.HostAttacksMain
import jobOneHour.subJob.MaliciousCode.MaliciousCodeMain
import jobOneHour.subJob.SQLInjectionAttacks.SQLInjectionAttacksMain
import jobOneHour.subJob.detectDGA.dgaDetectMain
import jobOneHour.subJob.dnsCovDetect.DNSCovMain
import jobOneHour.subJob.trafficAnomaly.anomalyTrafficMain
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
  * Created by Administrator on 2018/6/12.
  */
object jobOneHourMain extends LoggerSupport{

  def main(args: Array[String]): Unit = {


    /**
      * 配置日志
      */
    Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
//    var directory = new File("..")
//    PropertyConfigurator.configure(directory + "/conf/log4j_jobOneHour.properties")

    /**
      * 配置文件
      */
    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/jobOneHour.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/jobOneHour.properties")))
    var properties: Properties = new Properties()
    properties.load(ipstream)

    //计算任务超时次数
    var alarm_count_all = 0
    var alarm_count_day = 0


    /**
      * 建立spark会话
      */
//    val spark = getSparkSession(properties)

    while (true) {
      val time = get_day(format = "mm")
      val flag = time == properties.getProperty("run.time")
      val time_clean = get_day(format = "HH:mm")
      if(time_clean == "00:00"){
        alarm_count_day = 0   //清空当天告警数
      }


      if (flag) {

        val spark = getSparkSession(properties)
        val log_begin = s"start >> time >> ${get_day()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
        /**
          * 读取数据
          */
        val time_read_data_begin = new Date().getTime //记录开始运行时间（毫秒）
        val read_class = new methodReadData(spark, properties, logger)
        val (last_hour_netflow, data_netflow) = Try(read_class.readDataFromHDFS(data_source = "netflow")).getOrElse((null, null)) //获取处理后的netflow数据
        val (last_hour_dns, data_dns) = Try(read_class.readDataFromHDFS(data_source = "dns")).getOrElse((null, null)) //获取处理后的dns数据
        val (last_hour_http, data_http) = Try(read_class.readDataFromHDFS(data_source = "http")).getOrElse((null, null)) //获取处理后的dns数据
//        val (last_hour_netflow, data_netflow) = read_class.readDataFromHDFS(data_source = "netflow") //获取处理后的netflow数据
//        val (last_hour_dns, data_dns) = read_class.readDataFromHDFS(data_source = "dns") //获取处理后的dns数据
//        val (last_hour_http, data_http) = read_class.readDataFromHDFS(data_source = "http") //获取处理后的dns数据


        val time_read_data_end = new Date().getTime   //记录结束运行时间（毫秒）
        val time_read_data = (time_read_data_end - time_read_data_begin) / 1000 //记录整体运行时间（毫秒）

        /**
          * 调度模型
          */
        var time_anomaly_traffic:Long = 0 //记录s异常流量运行时间
        var time_heart_beat:Long = 0  //记录间歇性会话运行时间
        if (last_hour_netflow != null && data_netflow != null) {
          try {
            data_netflow.persist(StorageLevel.MEMORY_AND_DISK_SER) //缓存
            println(">>>>>>>>>>>>>>>>>>>>>>>>> 异常流量 >>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_anomaly_traffic_s = get_time()
            val anomaly_traffic = new anomalyTrafficMain(data_netflow, last_hour_netflow, spark, properties) //异常流量
            anomaly_traffic.main()
            val time_anomaly_traffic_e = get_time()
            time_anomaly_traffic = (time_anomaly_traffic_e - time_anomaly_traffic_s) / 1000
            println(">>>>>>>>>>>>>>>>>>>>>>>>> 间歇性会话 >>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_heart_beat_s = get_time()
            val heart_beat = new heartbeat_autocorrelation(spark, data_netflow, properties) //间歇性会话
            heart_beat.heartbeatPredict()
            val time_heart_beat_e = get_time()
            time_heart_beat = (time_heart_beat_e - time_heart_beat_s) / 1000
          } catch {
            case e: Exception => logger.error(e.getMessage)
          }
        }
//
        var time_dga_detect:Long = 0  //记录DGA检测攻击运行时间
        var time_dns_cov:Long = 0 //记录隐蔽通道运行时间
        if (last_hour_dns != null && data_dns != null) {
          try {
            data_dns.persist(StorageLevel.MEMORY_AND_DISK_SER) //缓存
            println(">>>>>>>>>>>>>>>>>>>>>>>>> DGA检测 >>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_dga_detect_s = get_time()
            val dga_detect = new dgaDetectMain(data_dns, last_hour_dns, spark, properties) //DGA检测
            dga_detect.main()
            val time_dga_detect_e = get_time()
            time_dga_detect = (time_dga_detect_e - time_dga_detect_s) / 1000
            println(">>>>>>>>>>>>>>>>>>>>>>>>> 隐蔽信道 >>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_dns_cov_s = get_time()
            val dns_cov = new DNSCovMain(properties, spark, data_dns) //隐蔽信道
            dns_cov.main()
            val time_dns_cov_e = get_time()
            time_dns_cov = (time_dns_cov_e - time_dns_cov_s) / 1000
          } catch {
            case e: Exception => logger.error(e.getMessage)
          }
        }

        var time_sql_attack:Long = 0  //记录sql攻击运行时间
        var time_xss_attack:Long = 0  //记录xss攻击运行时间
        if (last_hour_http != null && data_http != null) {
          try {
            data_http.persist(StorageLevel.MEMORY_AND_DISK_SER) //缓存
            println(">>>>>>>>>>>>>>>>>>>>>>>>> SQL注入 >>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_sql_attack_s = get_time()
            val sql_attack = new SQLInjectionAttacksMain(properties, spark, data_http) //SQL注入
            sql_attack.main()
            val time_sql_attack_e = get_time()
            time_sql_attack = (time_sql_attack_e - time_sql_attack_s) / 1000
            println(">>>>>>>>>>>>>>>>>>>>>>>>> XSS注入 >>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_xss_attack_s = get_time()
            val xss_attack = new MaliciousCodeMain(properties, spark, data_http) //XSS注入
            xss_attack.main()
            val time_xss_attack_e = get_time()
            time_xss_attack = (time_xss_attack_e - time_xss_attack_s) / 1000
          } catch {
            case e: Exception => logger.error(e.getMessage)
          }
        }

        var time_host_attack:Long = 0
        try {
          println(">>>>>>>>>>>>>>>>>>>>>>>>> 主机沦陷计分 >>>>>>>>>>>>>>>>>>>>>>>>>")
          val time_host_attack_s = get_time()
          val host_attack = new HostAttacksMain(properties, spark) //主机沦陷计分
          host_attack.main()
          val time_host_attack_e = get_time()
          time_host_attack = (time_host_attack_e - time_host_attack_s) / 1000
        } catch {
          case e: Exception => logger.error(e.getMessage)
        }

        /**
          * 调度日志
          */
        if((last_hour_netflow != null && data_netflow != null) || (last_hour_dns != null && data_dns != null) || (last_hour_http != null && data_http != null)) {
          //总运行时间
          val time_all = time_read_data +
            time_anomaly_traffic +
            time_heart_beat +
            time_dga_detect +
            time_dns_cov +
            time_sql_attack +
            time_xss_attack +
            time_host_attack

          //计算任务超时次数
          if (time_all >= 3600) {
            alarm_count_day += 1
            alarm_count_all += 1
          }
          //撰写日志
          val log_data = s"cost time >> read ori data >> $time_read_data s\n"
          val log_anomaly_traffic = s"cost time >> anomaly_traffic >> $time_anomaly_traffic s\n"
          val log_heart_beat = s"cost time >> heart_beat >> $time_heart_beat s\n"
          val log_dga_detect = s"cost time >> dga_detect >> $time_dga_detect s\n"
          val log_dns_cov = s"cost time >> dns_cov >> $time_dns_cov s\n"
          val log_sql_attack = s"cost time >> sql_attack >> $time_sql_attack s\n"
          val log_xss_attack = s"cost time >> xss_attack >> $time_xss_attack s\n"
          val log_host_attack = s"cost time >> host_attack >> $time_host_attack s\n"
          val log_sum = s"cost sum >> ${time_all / 60} m ${time_all % 60} s\n"
          val log_count_day = s"当天任务超时次数 >> $alarm_count_day\n"
          val log_count_sum = s"总任务超时次数次数 >> $alarm_count_all\n"
          val log_end = s"end >> time >> ${get_day()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n"
          val log_all = log_begin +
            log_data +
            log_anomaly_traffic +
            log_heart_beat +
            log_dga_detect +
            log_dns_cov +
            log_sql_attack +
            log_xss_attack +
            log_host_attack +
            log_sum +
            log_count_day +
            log_count_sum +
            log_end
          //写文件
          val targetFile = properties.getProperty("log.path")
          wirte_file(log_all, targetFile)
        }

        if(data_netflow != null) {
          data_netflow.unpersist()
        }
        if(data_dns != null) {
          data_dns.unpersist()
        }
        if(data_http != null) {
          data_http.unpersist()
        }
        Thread.sleep(60000) //1分钟
        spark.stop()

      }

    }
  }

  //配置spark
  def getSparkSession(properties: Properties): SparkSession = {
    val sparkconf = new SparkConf()
      .setMaster(properties.getProperty("spark.master.url"))
      .setAppName(properties.getProperty("spark.app.name"))
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
      .set("spark.sql.shuffle.partitions", properties.getProperty("spark.sql.shuffle.partitions"))
      .set("spark.sql.crossJoin.enabled", properties.getProperty("spark.sql.crossJoin.enabled"))
      .set("spark.sql.codegen", properties.getProperty("spark.sql.codegen"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }

   //获取当前时间(Long)
   def get_time() = {
     val time = new Date().getTime
     time
   }

   //获取当前时间(String)
   def get_day(format:String = "yyyy-MM-dd HH:mm:ss") = {
     val time_long = new Date().getTime
     val day = new SimpleDateFormat(format).format(time_long)
     day
   }

  //写文件
  def wirte_file(content:String, path:String, add:Boolean = true) = {
    val file = new File(path).exists()  //判断条件
    if(!file){  //不存在则新建文件
      new File(path).createNewFile()
    }
    val fileW = new FileWriter(path, add)
    fileW.write(content)
    fileW.close()
  }

}
