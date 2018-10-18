package SituationAwareness_2_0a.RebuildLeak_Detect

import java.util.Date

import SituationAwareness2.CudeHDFS._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileSystem => hdfs_fs, Path => hdfs_path}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 有构建的基线包裹两个东西
  * 一是用户用过什么类型的数据，用过多少次
  * 二是，在什么时间段，用了多大的数据量（？？？）
  *
  * 应当支持如下功能：
  * 从文件中读取基线。
  * 向文件中添加新的基线数据。
  *
  * 主要支持的函数有：
  * scan_seg_files：扫描并发现新的已分词的文件，把新的部分加进基线里。
  * grant_baseline：读取基线，把基线的计算结果存到time_period_amount和tag_times里。
  *
  * 2018-7-11改动：在保存基线时，变为仅保存当前处理的基线。在读取时，将所有的基线文件全部读取出来、
  * 2018-7-12改进：需要增加copy方法。
  */
class RebuildUserBaseline(rtj: RebuildTagJudge, hu: HDFSUtils) {
  var seg_dir = ""
  var baseline_path = ""
  var md5_path = ""
  var baseline_is_train = false

  /**
    * 有几个部分组成：用户ip，主题，哪一天，时间段编号，数据大小
    * 保存时就储存这个玩意
    */
  var user_tag_times = ArrayBuffer[(String, String, String, String, Int)]()
  var cache_user_tag_times = ArrayBuffer[(String, String, String, String, Int)]()
  var time_period_amount = Map[(String, String, String), Int]()
  var tag_times = Map[(String, String), Int]()
  var user_times = Map[String, Int]()

  def standard_int(n: Int): String = {
    var res = n.toString
    if (res.length == 1)
      res = "0" + res
    res
  }

  /**
    * 暂时先不改了。
    *
    * @param filename
    * @return
    */
  def seg_filename(filename: String): (String, String, String, String, String, String) = {
    val strs = filename.replaceAll("_", " _ ").split("_").map(_.trim)
    //处理时间
    val timestamp = new Date(strs(0).toLong)
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

    (from, to, src_port_option, dst_port_option, date, hour.toString)
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

  /**
    * 每个seg_str就是已经分好词的一条数据。
    * 按照目前的格式，用\t划分成三段：文件名，标题，正文
    * 把md5流转也写到这里来吧
    *
    * @param seg_str
    */
  def cache_info(seg_str: String): Unit = {
    val strs = seg_str.split("\\t")
    val file_name = strs(0)
    val title = strs(1)
    val words = seg_str.substring(file_name.length + title.length + 2).split(" ")
    val file_size = words.map(_.getBytes().length).sum
    val (res, _) = rtj.judge(words, false)
    if (res.length > 0)
      for (tag <- res) {
        println(file_name)
        val (from, to, src_port_option, dst_port_option, date, hour) = seg_filename(file_name)
        for (ip <- Array(from, to))
          if (is_inner(ip)) {
            cache_user_tag_times += ((ip, tag, date, hour, file_size))
          }
        //计算一下MD5
      }
  }

  /**
    * 从user_tag_times计算基线
    */
  def grant_baseline(): Unit = {
    load()
    val tmr_start = System.currentTimeMillis()
    tag_times = user_tag_times.map(line => (line._1, line._2))
      .groupBy(line => (line._1, line._2))
      .map(line => (line._1, line._2.length))
    tag_times.take(5).foreach(println)
    //简单粗暴的取最大值
    time_period_amount = user_tag_times.map(line => (line._1, line._2, line._4, line._5))
      .groupBy(line => (line._1, line._2, line._3))
      .map(line => (line._1, line._2.map(_._4).max))
    time_period_amount.take(5).foreach(println)
    user_times = user_tag_times.map(_._1).groupBy(ele => ele)
      .map(line => (line._1, line._2.length))
    val tmr_end = System.currentTimeMillis()
    println(s"计算基线，耗时${(tmr_end - tmr_start) / 1000.0}s")
  }

  /**
    * 主动调用这个东西来生成这段时间里的基线。
    *
    * @param files 一串hdfs上的文件的名称
    */
  def scan_seg_files(files: Array[String]): Unit = {
    val tmr_start = System.currentTimeMillis()
    cache_user_tag_times = ArrayBuffer[(String, String, String, String, Int)]() //别重复的写基线
    for (file <- files) {
      val br = hu.open_br(file)
      var line = br.readLine()
      while (line != null) {
        cache_info(line)
        line = br.readLine()
      }
      br.close()
    }

    val tmr_end = System.currentTimeMillis()
    println(s"遍历文件，用时${(tmr_end - tmr_start) / 1000.0}s")
    if (baseline_is_train) save()
  }

  /**
    * 文件头不再需要记录last了
    * baseline_path变为一个文件夹。需要把文件夹里的全部文件都读取出来
    */
  def load(): Unit = {
    val tmr_start = System.currentTimeMillis()
    user_tag_times = ArrayBuffer[(String, String, String, String, Int)]()

    if (hu.hdfs.exists(new hdfs_path(baseline_path)))
      if (hu.hdfs.isDirectory(new hdfs_path(baseline_path)))
        for (file_path <- hu.list_hdfs_file(baseline_path)) {
          val br = hu.open_br(file_path)
          var line = br.readLine()
          //      line = br.readLine()
          while (line != null) {
            val strs = line.split("\\t")
            user_tag_times += ((strs(0), strs(1), strs(2), strs(3), strs(4).toInt))
            line = br.readLine()
          }
          br.close()

        }
    user_tag_times.take(5).foreach(println)
    val tmr_end = System.currentTimeMillis()
    println(s"读入基线，用时${(tmr_end - tmr_start) / 1000.0}s")
  }

  /**
    * 文件头不再需要记录last了
    * 生成一个唯一标识，来作为文件名
    */
  def save(): Unit = {
    val md5 = DigestUtils.md5Hex(System.currentTimeMillis().toString + new Random().nextInt(10000).toString)
    val tmr_start = System.currentTimeMillis()
    var fw = hu.open_bw(baseline_path + md5 + ".txt")
    val sum_user_tag = cache_user_tag_times.map(line => ((line._1, line._2, line._3, line._4), line._5))
      .groupBy(_._1)
      .map(line => (line._1._1, line._1._2, line._1._3, line._1._4, line._2.map(_._2).sum))
    for (record <- sum_user_tag) {
      val write_str = s"${record._1}\t${record._2}\t${record._3}\t${record._4}\t${record._5.toString}"
      fw.write(write_str + "\r\n")
    }
    fw.close()
    /**
      * 记录md5文件流转
      * 直接记录到Kafka上去
      */

    //    val (sftp, ssh_session, channel) = new SftpUtils().open_channel(jsch_ip, jsch_user, jsch_psw)
    //    new SftpUtils().sftp_mkdirs(jsch_md5_path, sftp)
    //    val os = sftp.put(jsch_md5_path + md5 + ".txt")
    //    fw = new BufferedWriter(new OutputStreamWriter(os))
    //    for (record <- cache_md5) {
    //      val write_str = s"${record._1}\t${record._2}\t${record._3}"
    //      fw.write(write_str + "\r\n")
    //    }
    //    fw.close()
    //    sftp.disconnect()
    //    channel.disconnect()
    //    ssh_session.disconnect()
    //------------------------------
    val tmr_end = System.currentTimeMillis()
    println(s"保存基线，用时${(tmr_end - tmr_start) / 1000.0}s")
  }

  def copy(): RebuildUserBaseline = {
    val rub = new RebuildUserBaseline(rtj, hu)
    rub.seg_dir = seg_dir
    rub.baseline_path = baseline_path
    rub.md5_path = md5_path
    rub.baseline_is_train = baseline_is_train

    rub.user_tag_times = user_tag_times
    rub.cache_user_tag_times = cache_user_tag_times
    rub.time_period_amount = time_period_amount
    rub.tag_times = tag_times
    rub.user_times = user_times

    rub
  }


  def main(args: Array[String]): Unit = {

    println(user_tag_times.length)
    grant_baseline()
    //    println(seg_filename("1526284476354449-172.16.2.104_45160-121.12.98.85_80-0-tc.data"))
  }
}
