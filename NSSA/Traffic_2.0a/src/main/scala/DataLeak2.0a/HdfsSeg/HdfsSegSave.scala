package SituationAwareness_2_0a.HdfsSeg

import SituationAwareness2.CudeHDFS._
import org.ansj.splitWord.analysis.NlpAnalysis

class HdfsSegSave(hu: HDFSUtils, hdfs_path: String, hdfs_save_path: String,
                  date: String, hour: String, minute: String, nlpAnalysis: NlpAnalysis) extends Runnable {


  def work(): Unit = {

    val files = hu.list_hdfs_file(hdfs_path + "/" + date + "/" + hour + "/" + minute)
    println(s"check ${date} ${hour} ${minute}")
    files.foreach(println)
    if (files.nonEmpty) {
      val bw = hu.open_bw(hdfs_save_path + s"seg-${date}-${hour}-${minute}.txt")
      println(s"prepare write to ${hdfs_save_path + s"seg-${date}-${hour}-${minute}.txt"}")
      for (file <- files)
        try {
          val br = hu.open_br(file)
          var line = br.readLine()
          //第一行是文件名，特别处理一下
          val parts = file.split("/")
          var write_str = parts.last + "\t" + line + "\t"
          line = br.readLine()
          while (line != null) {
            val parse = nlpAnalysis.parseStr(line)
            val words = for (i <- 0 until parse.getTerms.size()) yield parse.get(i).getName
            write_str += (if (words.nonEmpty) words.map(ele => ele + " ").reduce(_ + _) else "")

            line = br.readLine()
          }
          br.close()
          write_str += "\r\n"
          bw.write(write_str)
        }
        catch {
          case e: Exception =>
            println("读文件时死掉了")
            e.printStackTrace()
        }
      bw.close()
    }
  }

  override def run(): Unit = {
    try {
      work()
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
