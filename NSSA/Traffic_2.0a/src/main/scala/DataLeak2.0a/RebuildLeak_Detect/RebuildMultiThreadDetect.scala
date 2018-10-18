package SituationAwareness_2_0a.RebuildLeak_Detect

import java.io._
import java.util.concurrent.Executors
import java.util.{Date, Properties}

import SituationAwareness2.CudeHDFS._
import org.apache.hadoop.fs.{FileSystem => hdfs_fs, Path => hdfs_path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext}

/**
  * 开启多线程，来调用RebuildDetectFromSeq
  */
@DisallowConcurrentExecution
class RebuildMultiThreadDetect extends Job {
  var date_start = ""
  var date_end = ""
  var hdfs_ip = ""
  var scan_dir = ""
  var model_path = ""
  var result_path = ""
  var baseline_path = ""
  var md5_path = ""
  var jsch_ip = ""
  var jsch_user = ""
  var jsch_psw = ""
  var jsch_leak_result_path = ""
  var jsch_md5_path = ""
  var encrypt_path = ""
  var spark_master = ""
  var spark_app_name = ""
  var spark_max_retry = ""
  var baseline_is_train = false
  var current_hour = ""

  val hu = new HDFSUtils
  var spark: SparkSession = null

  var period_files = Map[(String, String), Array[String]]()

  def load_Properties(): Unit = {
    val properties = new Properties()
    val path = new File(new File(".").getAbsolutePath + "/conf/troublesome.properties").getAbsolutePath //这个是用在服务器上的？
    println(path)
    //    val path = "conf/troublesome.properties"
    properties.load(new FileInputStream(path))
    date_start = properties.getProperty("hgh.detect.date_start")
    date_end = properties.getProperty("hgh.detect.date_end")
    hdfs_ip = properties.getProperty("hgh.hdfs.ip")
    scan_dir = properties.getProperty("hgh.detect.scan_dir")
    model_path = properties.getProperty("hgh.light_lda.model_path")
    result_path = properties.getProperty("hgh.light_lda.result_path")
    baseline_path = properties.getProperty("hgh.light_lda.baseline_path")
    md5_path = properties.getProperty("hgh.light_lda.md5_path")
    jsch_ip = properties.getProperty("hgh.detect.jsch_ip")
    jsch_user = properties.getProperty("hgh.detect.jsch_user")
    jsch_psw = properties.getProperty("hgh.detect.jsch_pwd")
    jsch_leak_result_path = properties.getProperty("hgh.detect.jsch_leak_result_path")
    jsch_md5_path = properties.getProperty("hgh.detect.jsch_md5_path")
    encrypt_path = properties.getProperty("hgh.light_lda.crypt_path")
    baseline_is_train = properties.getProperty("hgh.baseline.is_train").toLowerCase() == "true"

    spark_master = properties.getProperty("hgh.spark.master_domain")
    spark_app_name = properties.getProperty("hgh.spark.app_name")
    spark_max_retry = properties.getProperty("hgh.spark.max_retries")

    hu.init(hdfs_ip)
  }

  def spark_init(): Unit = {
    val spark_conf = new SparkConf()
      .setMaster(spark_master)
      .setAppName(spark_app_name)
      .set("spark.port.maxRetries", spark_max_retry)
    spark = SparkSession.builder().config(spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  def split_files(): Unit = {
    val dir = hu.list_hdfs_file(hdfs_ip + scan_dir)
    for (file <- dir.filter(f => hu.hdfs.isFile(new hdfs_path(f)))) {
      val file_name = new hdfs_path(file).getName
      val strs = file_name.split("-")
      val date = strs(1)
      val hour = strs(2)
      val minute = strs(3)
      //      println(file_name, date, hour)
      //由于经过手动设置，实际上变成了取出某天，某个小时内的文件
      if (date.toInt >= date_start.toInt && date.toInt <= date_end.toInt && hour == current_hour) {
        val key = (date, hour)
        println(file_name, date, hour)
        if (period_files.contains(key))
          period_files += (key -> (period_files(key) :+ file))
        else
          period_files += (key -> Array(file))
      }
    }
    period_files.take(5).map(line => (line._1, line._2.length, line._2.reduce(_ + "\r\n" + _))).foreach(println)
  }

  def start_threads(): Unit = {
    split_files()
    val rtj = new RebuildTagJudge
    rtj.init(hu.open_br(hdfs_ip + result_path + model_path))
    val rub = new RebuildUserBaseline(rtj, hu)
    //    rub.load()  //grant_baseline有load了啊。。。。
    rub.baseline_path = hdfs_ip + result_path + baseline_path
    rub.md5_path = hdfs_ip + result_path + md5_path
    rub.seg_dir = hdfs_ip + scan_dir
    rub.baseline_is_train = baseline_is_train
    rub.grant_baseline()
    val pool = Executors.newFixedThreadPool(20)
    for (line <- period_files) {
      val hour = line._1._2
      val files = line._2
      val rdfs = new RebuildDetectFromSeq(rtj, rub, hu, spark)
      rdfs.jsch_ip = jsch_ip
      rdfs.jsch_user = jsch_user
      rdfs.jsch_psw = jsch_psw
      rdfs.jsch_leak_result_path = jsch_leak_result_path
      rdfs.encrypt_path = encrypt_path
      rdfs.set_files_period(files, hour)
      pool.execute(rdfs)
    }
    pool.shutdown()
  }

  def main(args: Array[String]): Unit = {
    load_Properties()
    spark_init()

    current_hour = "15"
    date_start = "20180716"
    date_end = "20180716"
    //    split_files()
    start_threads()
  }

  def standard_int(i: Int): String = {
    var res = i.toString
    if (res.length == 1)
      res = "0" + res
    res
  }

  override def execute(context: JobExecutionContext): Unit = {
    val schedule_data_map = context.getJobDetail.getJobDataMap


    load_Properties()
    spark_init()

    val offset = schedule_data_map.getString("offset").toLong
    val target_time = new Date(System.currentTimeMillis() - offset)
    val year = target_time.getYear + 1900
    val month = target_time.getMonth + 1
    val day = target_time.getDate
    val hour = target_time.getHours
    val date = year.toString + standard_int(month) + standard_int(day)

    //    current_hour = standard_int(hour)
    //    date_start = date
    //    date_end = date
    current_hour = "15"
    date_start = "20180731"
    date_end = "20180731"
    println(s"准备启动的时间：${current_hour},${date_end},${date_start}")

    start_threads()
    println("处理完成")
    Thread.sleep(1000000000)
  }
}
