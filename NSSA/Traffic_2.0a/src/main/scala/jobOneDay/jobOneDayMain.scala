package jobOneDay

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.util.Properties

import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Try
import jobOneDay.subClass._
import jobOneDay.subJob.BotnetDetectBaseOnDNS.botnetDetectDNS
import jobOneDay.subJob.CleanData.CleanDataMain
import jobOneDay.subJob.NetflowBotNet.NetflowBotNetMain
import jobOneDay.subJob.RecoverNetflowData.recoverNetflowMain
import jobOneDay.subJob.RecoverUpdownData.RecoverUpdown
import jobOneDay.subJob.ScoreWeightCalculate.scoreComputeMain
import org.apache.spark.storage.StorageLevel

/**
  * Created by Administrator on 2018/6/12.
  */

object jobOneDayMain extends LoggerSupport{

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
//    var directory = new File("..")
//    PropertyConfigurator.configure(directory + "/conf/log4j_jobOneDay.properties")

    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/jobOneDay.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/jobOneDay.properties")))
    var properties: Properties = new Properties()
    properties.load(ipstream)



    //计算任务超时次数
    var alarm_count_all = 0
    var alarm_count_day = 0

    while (true) {
      val time = get_day(format = "HH:mm")
      val flag = time == properties.getProperty("run.time")
      if (flag) {
        val log_begin = s"start >> time >> ${get_day()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
        alarm_count_day = 0 //清空当天告警数
        /**
          * 建立spark会话
          */
        val spark = getSparkSession(properties)

        /**
          * 读取数据
          */
        val time_read_data_begin = new Date().getTime //记录开始运行时间（毫秒）
        val read_class = new methodReadData(spark, properties, logger)
//        val (last_day_netflow, data_netflow) = read_class.readDataFromHDFS(data_source = "netflow") //获取处理后的netflow数据
//        val (last_day_dns, data_dns) = read_class.readDataFromHDFS(data_source = "dns") //获取处理后的dns数据
        val time_read_data_end = new Date().getTime
        //记录结束运行时间（毫秒）
        val time_read_data = (time_read_data_end - time_read_data_begin) / 1000 //记录整体运行时间（毫秒）

        /**
          * 调度模型
          */
        var time_clean_data:Long = 0  //记录数据清理 时间
        try {
          println("清理数据>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          val time_clean_data_s = get_time()
          val clean_data = new CleanDataMain(properties, spark) //清理数据
          clean_data.main()
          val time_clean_data_e = get_time()
          time_clean_data = (time_clean_data_e - time_clean_data_s) / 1000
        } catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }


        var time_botnet_dns:Long = 0  //记录基于DNS协议的僵尸网络检测 时间
//        data_dns.persist(StorageLevel.MEMORY_AND_DISK_SER)
        try {
          println("僵尸网络DNS>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          val time_botnet_dns_s = get_time()
          val botnet_dns = new botnetDetectDNS(spark, properties) //僵尸网络DNS
          botnet_dns.main()
          val time_botnet_dns_e = get_time()
          time_botnet_dns = (time_botnet_dns_e - time_botnet_dns_s) / 1000
        }catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }

        var time_botnet_netflow:Long = 0  //记录基于netflow的僵尸网络检测 时间
        try {
          println("僵尸网络NETFLOW>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
//          data_netflow.persist(StorageLevel.MEMORY_AND_DISK_SER)
          val time_botnet_netflow_s = get_time()
          val botnet_netflow = new NetflowBotNetMain(properties, spark) //僵尸网络NETFLOW
          botnet_netflow.main()
          val time_botnet_netflow_e = get_time()
          time_botnet_netflow = (time_botnet_netflow_e - time_botnet_netflow_s) / 1000
        }catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }


        var time_score_compute:Long = 0 //记录主机沦陷权重计算 时间
        try {
          println("主机沦陷权重计算>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          val time_score_compute_s = get_time()
          val score_compute = new scoreComputeMain(spark, properties)//主机沦陷权重计算
          score_compute.main()
          val time_score_compute_e = get_time()
          time_score_compute = (time_score_compute_e - time_score_compute_s) / 1000
        }catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }


        var time_recover_netflow:Long = 0 //记录覆盖netflow数据脚本 时间
        try{
          println("覆盖netflow数据脚本 >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
          val time_recover_netflow_s = get_time()
          val recover_netflow = new recoverNetflowMain(spark, properties)//覆盖netflow数据脚本
          recover_netflow.main()
          val time_recover_netflow_e = get_time()
          time_recover_netflow = (time_recover_netflow_e - time_recover_netflow_s) / 1000
        }catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }


        var time_recover_up_down:Long = 0 //记录覆盖上下行脚本 时间
        try{
          val time_recover_up_down_s = get_time()
          val recover_netflow = new RecoverUpdown(spark, properties)//覆盖上下行脚本
          recover_netflow.recoverupdown()
          val time_recover_up_down_e = get_time()
          time_recover_up_down = (time_recover_up_down_e - time_recover_up_down_s) / 1000
        }catch {
          case e: Exception => logger.error("出错：" + e.getMessage)
        }


        /**
          * 调度日志
          */
        //总运行时间
        val time_all = time_read_data +
          time_clean_data +
          time_botnet_dns +
          time_botnet_netflow +
          time_score_compute +
          time_recover_netflow +
          time_recover_up_down


        //计算任务超时次数
        if (time_all >= 60 * 60 * 12) {
          alarm_count_day += 1
          alarm_count_all += 1
        }

        //撰写日志
        val log_data = s"cost time >> ${time_read_data / 60} m ${time_read_data % 60} s >> read ori data\n"
        val log_clean_data = s"cost time >> ${time_clean_data / 60} m ${time_clean_data % 60} s >> anomaly_traffic\n"
        val log_botnet_dns = s"cost time >> ${time_botnet_dns / 60} m ${time_botnet_dns % 60} s >> heart_beat\n"
        val log_botnet_netflow = s"cost time >> ${time_botnet_netflow / 60} m ${time_botnet_netflow % 60} s >> dga_detect\n"
        val log_score_compute = s"cost time >> ${time_score_compute / 60} m ${time_score_compute % 60} s >> dns_cov\n"
        val log_recover_netflow = s"cost time >> ${time_recover_netflow / 60} m ${time_recover_netflow % 60} s >> recover_netflow\n"
        val log_recover_updown = s"cost time >> ${time_recover_up_down / 60} m ${time_recover_up_down % 60} s >> recover_updown\n"
        val log_sum = s"cost sum >> ${time_all / 60} m ${time_all % 60} s\n"
        val log_count_day = s"当天任务超时次数 >> $alarm_count_day\n"
        val log_count_sum = s"总任务超时次数次数 >> $alarm_count_all\n"
        val log_end = s"end >> time >> ${get_day()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n"
        val log_all = log_begin +
          log_data +
          log_clean_data +
          log_botnet_dns +
          log_botnet_netflow +
          log_score_compute +
          log_recover_netflow +
          log_recover_updown +
          log_sum +
          log_count_day +
          log_count_sum +
          log_end

        //写文件
        val targetFile = properties.getProperty("log.path")
        wirte_file(log_all, targetFile)


        spark.stop()  //关闭spark连接
        Thread.sleep(60000) //1分钟
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
