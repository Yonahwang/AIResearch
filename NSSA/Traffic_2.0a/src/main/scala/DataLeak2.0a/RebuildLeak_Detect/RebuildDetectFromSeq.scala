package SituationAwareness_2_0a.RebuildLeak_Detect

import java.util.Date

import SituationAwareness2.CudeHDFS._
import SituationAwareness2.SenceDetect.Analyser
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileSystem => hdfs_fs, Path => hdfs_path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

/**
  * 计划做成多线程版。每启动一个本线程，需要传入一个时间段里（目前是一小时）的文件，进行处理
  * 目前不确定输出到哪个数据库，所以暂停。
  * 整个代码可以分割为3个部分的检测：
  * 1、检测是否包含敏感词
  * 2、检测是否浏览了以往少见的主题
  * 3、本时段内浏览总量是否异常高
  *
  * 2018-7-16todo:文件流转
  *
  * @param rtj   复用的主题推断模块。需要预加载。
  * @param a_rub 复用的基线构造模块。需要预加载。
  */
class RebuildDetectFromSeq(rtj: RebuildTagJudge, a_rub: RebuildUserBaseline, hu: HDFSUtils, spark: SparkSession)
  extends Runnable {
  val rub = a_rub.copy()
  var watch_len = 10
  var take = 3
  var word_views = 3
  var hour = ""
  var current_time = ""
  //  val rub = new RebuildUserBaseline(rtj)
  val thershold = 0.003
  var current_period_size = Map[(String, String), Int]() //用户，主题，数量
  var files = Array[String]()
  var cache_write = ArrayBuffer[String]()
  var words_cloud = Map[String, (Double, Long)]()
  //  var rtj: RebuildTagJudge = null
  var jsch_ip = ""
  var jsch_user = ""
  var jsch_psw = ""
  var jsch_leak_result_path = ""
  var encrypt_path = ""

  val analyser = new Analyser
  val goKafka = new SituationAwareness_2_0a.CudeHDFS.GoKafka

  def standard_int(n: Int): String = {
    var res = n.toString
    if (res.length == 1)
      res = "0" + res
    res
  }

  def set_files_period(files: Array[String], hour: String): Unit = {
    this.files = files
    this.hour = hour
  }

  def is_inner(ip: String): Boolean = {
    val reg ="""^172\.|^10\.""".r
    var res = false
    reg.findFirstIn(ip) match {
      case Some(s: String) => res = true
      case other => res = false
    }
    res
  }

  def seg_filename(filename: String): (String, String, String, String, String) = {
    "1526284476354449-172.16.2.104_45160-121.12.98.85_80-0-tc.data" //这是老版本的形式
    val strs = filename.replaceAll("_", " _ ").split("_").map(_.trim)
    //处理时间
    val time = strs(0)
    val timestamp = new Date(time.toLong)
    val year = timestamp.getYear + 1900
    val month = timestamp.getMonth + 1
    val day = timestamp.getDate
    val hour = timestamp.getHours
    val date = year.toString + standard_int(month) + standard_int(day)
    //
    val from = strs(1)
    val to = strs(2)
    val src_port_option = strs(3)
    val dst_port_option = strs(4)
    //
    val file_type = strs(5)
    val protocol = strs(6)
    val origin_size = strs(7)
    (time, from, to, src_port_option, dst_port_option)
  }

  /**
    * 根据传入的关键词，找出包含关键词的句子。默认包含至少1个关键词
    *
    * @param keywords  关键词
    * @param unseg_str 文本
    * @return
    */
  def eval_abstract(keywords: Array[String], unseg_str: String): Array[String] = {
    var res_arr = Array[String]()
    val sentences = unseg_str.split("""[\s.!?。！？“”…]""")
    for (sentence <- sentences) {
      breakable {
        for (key <- keywords)
          if (sentence.contains(key)) {
            val index = sentence.indexOf(key)
            val start_p = math.max(0, index - watch_len)
            val end_p = math.min(sentence.length, index + watch_len)
            res_arr = res_arr :+ sentence.substring(start_p, end_p) //把摘要缓存
            break()
          }
      }
    }
    res_arr = new Random().shuffle(res_arr.toList).toArray //乱序
    val res = res_arr.take(3)
    res
  }

  def check_forbid_words(md5: String,
                         sensitive_result: (String, String, String, String),
                         file_name: String,
                         unseg_str: String,
                         terms: Array[String],
                         origin_file_name: String): Unit = {
    if (sensitive_result._1 != "") {
      val (time, from, to, src_port_option, dst_port_option) = seg_filename(file_name)

      val (tags, _) = rtj.judge(terms, false)

      val words = sensitive_result._4.split(",")
      println("发现敏感数据！")
      val info = "关键词：" + words.reduce(_ + "\t" + _) +
        "\r\n上下文摘要：\r\n\t" + eval_abstract(words, unseg_str).reduce(_ + "\r\n\t" + _)

      goKafka.cache_save(file_name, tags, info, sensitive_result._3, 0,
        md5, 1, origin_file_name, "")

    }
  }

  /**
    * 传入一个已经分词的文件的字符串，进行识别
    * 分成两个部分，一是检测敏感词，二是检查数据基线
    *
    * 2018-7-27改进：需要有原文件名，而现在只有新文件名
    *
    * @param seged_str 以空格进行分割的字符串
    */
  def detect_a_file(seged_str: String): Unit = {
    val strs = seged_str.split("\\t")
    val file_name = strs(0)
    val origin_file_name = strs(1)
    val words = strs(2).split(" ")
    val unseg_str = seged_str.replace(" ", "")
    //    println(file_name, words.take(5).reduce(_ + "," + _))
    //先判断是不是敏感
    val sensitive_result = analyser.check(words)
    val md5 = DigestUtils.md5Hex(words.reduce(_ + _))
    check_forbid_words(md5, sensitive_result, file_name, unseg_str, words, origin_file_name)
    //然后判断是否违反数据基线
    check_baseline(md5, words, file_name, origin_file_name)
    //发送文件流转信息
    record_file_flows(md5, file_name, origin_file_name)
    //统计词云
    count_word_cloud(words)
  }

  /**
    * 需要调整权重，but how?
    *
    * @param words
    */
  def count_word_cloud(words: Array[String]): Unit = {
    val to_add = words.groupBy(ele => ele).map(ele => (ele._1, ele._2.length))
    for (word <- to_add)
      if (words_cloud.contains(word._1))
        words_cloud += (word._1 -> (words_cloud(word._1)._1 + word._2, words_cloud(word._1)._2 + 1))
      else
        words_cloud += (word._1 -> (word._2, 1L))
  }

  /**
    *
    * @param s
    * @return
    */
  def not_chinese(s: String): Boolean = {
    var res = true
    for (c <- s)
      if (c.toString.getBytes().length > 1 && res) {
        val ub = Character.UnicodeBlock.of(c)
        var check = false
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
          || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
          || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
          || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
          || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
          || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS)
          check = true
        res = check
      }
    res
  }

  def eval_word_cloud(): Unit = {
    val take = 200
    val arr = words_cloud.filter(_._2._1 > 0)
      .filter(_._2._2 > 5)
      .filter(_._1.length > 1)
      .filter(ele => not_chinese(ele._1))
      .map {
        ele =>
          val log = math.log(ele._2._1)
          val df = ele._2._2
          (ele._1, log / df, ele._2._1)
      }.toArray
      .sortWith((ele1, ele2) => ele1._2 > ele2._2)
      .take(take)

    val str_cloud = arr.map(ele => ele._1 + "\t" + ele._3)
    str_cloud.foreach(println)
    goKafka.cache_save("", Array[String](), "", "",
      0, "no md5", 2, "no origin file name", str_cloud.reduce(_ + " " + _))
  }

  def record_file_flows(md5: String,
                        file_name: String,
                        origin_file_name: String): Unit = {
    goKafka.cache_save(file_name, Array[String](), "", "", 0, md5, 0, origin_file_name, "")
  }

  /**
    * 暂时还缺少：应用，文件类型，泄露类型，协议，url
    *
    * @param words
    * @param file_name
    */
  def check_baseline(md5: String,
                     words: Array[String],
                     file_name: String,
                     origin_file_name: String): Unit = {
    val (tags, top_words) = rtj.judge(words, true)
    val (time, from, to, srcport, dstport) = seg_filename(file_name)
    if (tags.length > 0)
      for (tag <- tags) {
        val word_size = words.reduce(_ + _).getBytes.length
        for (user <- Array(from, to).filter(ele => is_inner(ele))) {
          var is_normal = false
          if (rub.tag_times.contains((user, tag)))
            if (rub.tag_times((user, tag)).toDouble / rub.user_times(user) > thershold)
              is_normal = true
          //不管是否正常，先统计本时段内，使用该主题的量
          if (current_period_size.contains((user, tag)))
            current_period_size += ((user, tag) -> (word_size + current_period_size((user, tag))))
          else
            current_period_size += ((user, tag) -> word_size)
          //如果不正常，那就报告
          if (!is_normal) {
            println(user + "违反数据基线！")
            val report_str = "关键词：" + top_words.map(_._1).take(3).reduce(_ + "\t" + _) +
              "\r\n上下文摘要：\r\n\t" +
              eval_abstract(top_words.map(_._1).take(3), words.reduce(_ + _)).reduce(_ + "\r\n\t" + _)

            goKafka.cache_save(file_name, tags, report_str, "0", 1,
              md5, 1, origin_file_name, "")
          }
        }
      }
  }

  /**
    * 在本时间段的所有数据都跑完之后，执行这个函数，来判断是否浏览某个主题过多
    * 在2.0中，这个被屏蔽了
    */
  def check_over_view(): Unit = {
    for (line <- current_period_size) {
      val user = line._1._1
      val tag = line._1._2
      val size = line._2
      if (rub.time_period_amount.contains((user, tag, hour)))
        if (rub.time_period_amount((user, tag, hour)) < size) {
          //有问题，报出
          val report_str = s"${user}在${tag}主题上产生了过度流量。" +
            s"他过往最多使用${rub.time_period_amount((user, tag, hour)).toString}，现在使用${size}"
          println(report_str)
          //有current_time
          val fake_filenem = current_time + s"_${user}_8.8.8.8_5000_80_doc_ftp.data"
          goKafka.cache_save(fake_filenem, Array(tag), report_str,
            "0", 1, DigestUtils.md5Hex(report_str), 1, "", "")
        }
    }
  }

  override def run(): Unit = {
    rub.scan_seg_files(files)
    analyser.encrypt_path = encrypt_path
    analyser.jsch_ip = jsch_ip
    analyser.jsch_user = jsch_user
    analyser.jsch_psw = jsch_psw
    for (file <- files)
      if (hu.hdfs.isFile(new hdfs_path(file))) {
        println("dealing " + new hdfs_path(file).getName)
        val br = hu.open_br(file)
        var line = br.readLine()
        while (line != null) {
          try {
            detect_a_file(line)
          }
          catch {
            case e: Exception =>
              e.printStackTrace()
          }
          line = br.readLine()
        }
        br.close()
      }
    //    check_over_view() //2.0里不搞这个了，麻烦
    eval_word_cloud()
    //debug中，暂时不发kafka
    goKafka.trans_rdd(spark)
  }

}
