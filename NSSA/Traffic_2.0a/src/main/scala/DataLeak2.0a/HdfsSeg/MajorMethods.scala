package SituationAwareness_2_0a.HdfsSeg

import java.io.{BufferedReader, File, FileInputStream}
import java.util.concurrent.Executors
import java.util.{Date, Properties}

import SituationAwareness2.CudeHDFS._
import org.ansj.library.DicLibrary
import org.ansj.splitWord.analysis.NlpAnalysis
import org.quartz.{DisallowConcurrentExecution, Job, JobExecutionContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 时间和小时，由调用这个玩意的schedule提供吧.
  */
@DisallowConcurrentExecution
class MajorMethods extends Job {
  var start_day = ""
  var end_day = ""
  var hdfs_ip = ""
  var raw_files_path = ""
  var seg_files_path = ""
  var lda_result = ""
  var lda_specail_class = ""
  var eval_days = Array[String]()

  def load_Properties(): Unit = {
    val properties = new Properties()
    val path = new File(new File(".").getAbsolutePath + "/conf/troublesome.properties").getAbsolutePath //这个是用在服务器上的？
    //    println(path)
    //    val path = "conf/troublesome.properties"
    properties.load(new FileInputStream(path))
    start_day = properties.getProperty("hgh.seg_hdfs.start_day")
    end_day = properties.getProperty("hgh.seg_hdfs.end_day")
    hdfs_ip = properties.getProperty("hgh.hdfs.ip")
    raw_files_path = properties.getProperty("hgh.hdfs.raw_files")
    seg_files_path = properties.getProperty("hgh.hdfs.seg_files")

    lda_result = properties.getProperty("hgh.light_lda.result_path")
    lda_specail_class = properties.getProperty("hgh.light_lda.self_def_class")
    build_days()
  }

  @deprecated
  def build_days(): Unit = {
    val days_in_month = Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    val year_s = start_day.substring(0, 4).toInt
    val month_s = start_day.substring(4, 6).toInt
    val day_s = start_day.substring(6, 8).toInt
    println(year_s, month_s, day_s)
    val yesr_e = end_day.substring(0, 4).toInt
    val month_e = end_day.substring(4, 6).toInt
    val day_e = end_day.substring(6, 8).toInt
    var k = 1
    var current_day = start_day
    eval_days = eval_days :+ current_day
    while (current_day != end_day) {
      k += 1
      var year_c = current_day.substring(0, 4).toInt
      var month_c = current_day.substring(4, 6).toInt
      var day_c = current_day.substring(6, 8).toInt
      day_c += 1
      if (day_c > days_in_month(month_c - 1))
        if (month_c == 2) {
          var max = 28
          if (year_c % 4 == 0) max += 1
          if (year_c % 100 == 0 && year_c % 400 != 0) max -= 1
          if (day_c > max) {
            day_c = 1
            month_c += 1
            if (month_c == 13) {
              month_c = 1
              year_c += 1
            }
          }
        }
        else {
          day_c = 1
          month_c += 1
          if (month_c == 13) {
            month_c = 1
            year_c += 1
          }
        }
      current_day = year_c.toString +
        (if (month_c.toString.length == 1) "0" + month_c.toString else month_c.toString) +
        (if (day_c.toString.length == 1) "0" + day_c.toString else day_c.toString)
      eval_days = eval_days :+ current_day
      if (k > 1000) current_day = end_day
    }
    //    eval_days.foreach(println)
  }

  def create_nlp_parse(br: BufferedReader): NlpAnalysis = {
    var line = br.readLine()
    var all_words = ArrayBuffer[String]()
    while (line != null) {
      val strs = line.toLowerCase.split("\\t")
      if (strs.length == 3) {
        val class_name = strs(0)
        val train_or_not = if (strs(1) == "1") true else false
        val words = strs(2).split(",")
        all_words = all_words ++ words
      }
      line = br.readLine()
    }
    br.close()
    all_words = all_words.distinct
    all_words.foreach(println)
    val na = new NlpAnalysis()
    for (word <- all_words)
      DicLibrary.insert(DicLibrary.DEFAULT, word)
    na
  }

  def standard_int(i: Int): String = {
    var res = i.toString
    if (res.length == 1)
      res = "0" + res
    res
  }

  def main(args: Array[String]): Unit = {
    //    load_Properties()
    //    val hu = new HDFSUtils
    //    hu.init(hdfs_ip)
    //    val pool = Executors.newFixedThreadPool(20)
    //    for (date <- eval_days) {
    //      for (hour <- 12 until 13)
    //        for (minute <- 0 until 1) {
    //          pool.execute(new HdfsSegSave(hu, hdfs_ip + raw_files_path, hdfs_ip + seg_files_path,
    //            date, standard_int(hour), standard_int(minute)))
    //        }
    //    }
    //    pool.shutdown()
  }

  override def execute(context: JobExecutionContext): Unit = {

    val schedule_data_map = context.getJobDetail.getJobDataMap
    val offset = schedule_data_map.getString("offset").toLong
    val target_time = new Date(System.currentTimeMillis() - offset)
    val year = target_time.getYear + 1900
    val month = target_time.getMonth + 1
    val day = target_time.getDate
    var hour = target_time.getHours
    var date = year.toString + standard_int(month) + standard_int(day)


    //只是测试
    date = "20180709"
    hour = 12
    println(s"loading data from date:${date} hour:${hour}")
    //-----------------

    load_Properties()
    val hu = new HDFSUtils
    hu.init(hdfs_ip)
    val pool = Executors.newFixedThreadPool(20)
    println("init hdfsutils finished.")

    //在这里创建使用自定义词典的nlpanalysis
    println(s"grant dict from ${hdfs_ip + lda_result + lda_specail_class}")
    val br = hu.open_br(hdfs_ip + lda_result + lda_specail_class)
    val self_dict_np = create_nlp_parse(br)


    println(date, hour)
    for (minute <- 0 until 60) {
      try {
        pool.execute(new HdfsSegSave(hu, hdfs_ip + raw_files_path, hdfs_ip + seg_files_path,
          date, standard_int(hour), standard_int(minute), self_dict_np))
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    pool.shutdown()
    println(s"Job ${date} ${hour} finished.")
    Thread.sleep(1000000000)
  }
}
