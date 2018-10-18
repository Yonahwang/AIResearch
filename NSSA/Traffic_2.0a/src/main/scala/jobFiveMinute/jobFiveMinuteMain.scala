package jobFiveMinute

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.util.Properties
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.Try
import jobFiveMinute.subClass._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.lang.Long
import java.util.TimeZone
import jobFiveMinute.subJob.AbnormalPorts.AbnormalPortsMain
import jobFiveMinute.subJob.AssetAutoFind.assetFind
import jobFiveMinute.subJob.DataPrepare.DataPrepareMain
import jobFiveMinute.subJob.ExternalConnection.ExternalConnectionMain
import jobFiveMinute.subJob.Updown.UpdownPredict
import jobFiveMinute.subJob.netflowProcess.ntProcessMain
import org.apache.spark.storage.StorageLevel

import scala.tools.nsc.Global

/**
  * Created by Administrator on 2018/6/12.
  */

object jobFiveMinuteMain extends LoggerSupport{

  def main(args: Array[String]): Unit = {

    /**
      * 配置日志
      */
    Logger.getLogger("org").setLevel(Level.ERROR)    //显示的日志级别
//    var directory = new File("..")
//    PropertyConfigurator.configure(directory + "/conf/log4j_jobFiveMinute.properties")
//    Try(PropertyConfigurator.configure(new File("..") + "/conf/log4j_jobFiveMinute.properties"))
//      .getOrElse(PropertyConfigurator.configure(new File(".") + "/conf/log4j_jobFiveMinute.properties"))

    /**
      * 读取配置文件
      */
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("..") + "/conf/jobFiveMinute.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/jobFiveMinute.properties")))
    //默认读取前一个目录的conf，如果不存在，则读取本目录的conf
    var properties = new Properties()
    properties.load(ipstream)

    //记录超时告警次数
    var alarm_count_all = 0
    var alarm_count_day = 0

    /**
      * 建立spark会话
      */
//    val spark = getSparkSession(properties) //spark连接

    while (true) {
      val time = get_day(format = "mm").toDouble  //当前时刻的分钟
      val run_time = properties.getProperty("run.time").toInt //设置的运行时间间隔
      val flag = time % run_time == 0 //运行条件

      //清空当天超时告警数(当时间在区间00:00-00:05中时)
      val time_hm = get_day(format = "HH:mm")
      val delete_time_list = (0 to 5).map { x => "00:0" + x }.toList
      if (delete_time_list.contains(time_hm)) {
        alarm_count_day = 0
      }

      if (flag) {

        val spark = getSparkSession(properties) //spark连接
        /**
          * 读取原始数据 & 相关模型
          */
        val log_begin = s"start >> time >> ${get_day()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
        //        logger.error("start >> time >> "+get_day() + ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        println("原始数据的模型>>>>>>>>>>>>")
        val time_read_ori_data_begin = get_time()
        val read_class = new methodReadData(spark, properties)
        val (last_file, netflow_origin) = Try(read_class.readOriginHDFS(read_origin_data = true)).getOrElse((null, null))
        //获取原始netflow数据
        val time_read_ori_data_end = get_time()
        val time_read_ori_data = (time_read_ori_data_end - time_read_ori_data_begin) / 1000

        var time_assetfind: Long = 0
        //记录资产发现运行时间
        var time_dataprepare: Long = 0 //记录数据预处理运行时间
        if (last_file != null && netflow_origin != null) {
          try {
            netflow_origin.persist(StorageLevel.MEMORY_AND_DISK_SER) //缓存
            println("数据预处理>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_dataprepare_begin = get_time()
            val data_prepare = new DataPrepareMain(properties, spark, last_file) //数据预处理
            data_prepare.main()
            val time_dataprepare_end = get_time()
            time_dataprepare = (time_dataprepare_end - time_dataprepare_begin) / 1000

            println("资产发现>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_assetfind_begin = get_time()
            val asset = new assetFind(netflow_origin, spark, properties) //资产发现
            asset.main()
            val time_assetfind_end = get_time() //record time
            time_assetfind = (time_assetfind_end - time_assetfind_begin) / 1000
          } catch {
            case e: Exception => logger.error(e.getMessage)
          }
        }

        /**
          * 读取清理后数据 & 相关模型
          */
        println("清理数据后的模型>>>>>>>>>>>>")
        val time_read_data_begin = get_time()
        val (last_file_clean, netflow_clean) = Try(read_class.readOriginHDFS(read_origin_data = false, file_num = 3)).getOrElse((null, null))
        //获取处理后的netflow数据
        val time_read_data_end = get_time()
        val time_read_data = (time_read_data_end - time_read_data_begin) / 1000

        var time_abnormalport: Long = 0
        //记录端口异常运行时间
        var time_externalconn: Long = 0
        //记录主动外联运行时间
        var time_updown: Long = 0
        //记录上下行运行时间
        var time_abnomalnetflow: Long = 0 //记录异常流量运行时间
        if (last_file_clean != null && netflow_clean != null) {
          try {
            netflow_clean.persist(StorageLevel.MEMORY_AND_DISK_SER) //缓存
            println("端口异常>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_abnormalport_begin = get_time()
            val abnormal_port = new AbnormalPortsMain(properties, spark, netflow_clean) //端口异常
            abnormal_port.main()
            val time_abnormalport_end = get_time()
            time_abnormalport = (time_abnormalport_end - time_abnormalport_begin) / 1000
            println("主动外联>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_externalconn_begin = get_time()
            val external_connection = new ExternalConnectionMain(properties, spark, netflow_clean) //主动外联
            external_connection.main()
            val time_externalconn_end = get_time()
            time_externalconn = (time_externalconn_end - time_externalconn_begin) / 1000
            println("上下行>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_updown_begin = get_time()
            val up_down = new UpdownPredict(spark, netflow_clean, properties) //上下行
            up_down.UpdownPredictMain()
            val time_updown_end = get_time()
            time_updown = (time_updown_end - time_updown_begin) / 1000
            println("异常流量>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            val time_adnomalnetflow_begin = get_time()
            val abnomarl_netflow_process = new ntProcessMain(netflow_clean, last_file_clean, spark, properties) //异常流量
            abnomarl_netflow_process.main()
            val time_adnomalnetflow_end = get_time()
            time_abnomalnetflow = (time_adnomalnetflow_end - time_adnomalnetflow_begin) / 1000

          } catch {
            case e: Exception => logger.error(e.getMessage)
          }
        }
        val log_end = s"end >> time >> ${get_day()} >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n\n"


        /**
          * 调度日志
          */
        if ((last_file != null && netflow_origin != null) || (last_file_clean != null && netflow_clean != null)) {
          //总程序运行时间
          val time_all = time_read_ori_data + time_read_data + time_assetfind + time_dataprepare + time_abnormalport + time_externalconn + time_updown + time_abnomalnetflow

          //计算任务超时次数
          if (time_all >= 300) {
            alarm_count_day += 1
            alarm_count_all += 1
          }
          //撰写日志
          val log_ori_data = s"cost time >> read ori data >> $time_read_ori_data s\n"
          val log_data = s"cost time >> read data >> $time_read_data s\n"
          val log_asset = s"cost time >> asset find >> $time_assetfind s\n"
          val log_prepare = s"cost time >> data prepare >> $time_dataprepare s\n"
          val log_ap = s"cost time >> abnormal port >> $time_abnormalport s\n"
          val log_ec = s"cost time >> exterbal conn >> $time_externalconn s\n"
          val log_ud = s"cost time >> up down >> $time_updown s\n"
          val log_at = s"cost time >> abnomal traffic >> $time_abnomalnetflow s\n"
          val log_s = s"cost sum >> ${time_all / 60} m ${time_all % 60} s\n"
          val log_count_day = s"当天任务超时次数 >> $alarm_count_day\n"
          val log_count_sum = s"总任务超时次数次数 >> $alarm_count_all\n"
          val log_all = log_begin + log_ori_data + log_data + log_asset + log_prepare + log_ap + log_ec + log_ud + log_at + log_s + log_count_day + log_count_sum + log_end
          //写文件
          val targetFile = properties.getProperty("log.path")
          wirte_file(log_all, targetFile)
          //          logger.error("cost time >> read ori data >> "+ time_read_ori_data + " s")
          //          logger.error("cost time >> read data >> " + time_read_data + " s")
          //          logger.error("cost time >> asset find >> " + time_assetfind + " s")
          //          logger.error("cost time >> data prepare >> "+ time_dataprepare + " s")
          //          logger.error("cost time >> abnormal port >> "+ time_abnormalport + " s")
          //          logger.error("cost time >> exterbal conn >> "+ time_externalconn + " s")
          //          logger.error("cost time >> up down >> " + time_updown +" s")
          //          logger.error("cost time >> abnomal traffic >> " + time_abnomalnetflow + " s")
          //          logger.error("cost sum >> " + time_all / 60 + " m " + time_all % 60 + " s")
          //          logger.error("当天任务超时次数 >> "+ alarm_count_day)
          //          logger.error("总任务超时次数次数 >> " + alarm_count_all)
          //          logger.error("end >> time >> " + get_day() + " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n")

        }

        if(netflow_origin != null){
          netflow_origin.unpersist()  //清理缓存
        }
        if(netflow_clean != null){
          netflow_clean.unpersist()  //清理缓存
        }
        println("done>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        Thread.sleep(50000) //1分钟
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
