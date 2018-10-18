package SituationAwareness_2_0a.RebuildLeak_Detect

import java.io._

import org.apache.spark.ml.linalg.{Vector => s_Vector, Vectors => s_Vectors}

import scala.collection.mutable.ArrayBuffer

/**
  * 在init时就把模型都加载好，然后只调用judge来判别
  */
class RebuildTagJudge {
  var total_classes = 0
  val thershold = 0.3
  var word_vec = Map[String, s_Vector]()
  var index_tag = Map[Int, String]()
  var model_path = ""

  //  val rub = new RebuildUserBaseline

  /**
    * 需要加载词向量，和词的编号
    */
  def init(br: BufferedReader): Unit = {
    val tmr_start = System.currentTimeMillis()
    var word_no = Map[Int, String]()

    var line = br.readLine()
    var strs = line.split("\\t")
    val D = strs(0).toInt
    val V = strs(1).toInt
    val named_tag = strs(3).toInt
    var nkv = ArrayBuffer[Array[Int]]()
    //先取出nkv
    for (v <- 0 until V) {
      line = br.readLine()
      nkv += line.split(",").map(_.toInt)
    }
    //然后是取出词的序号
    for (v <- 0 until V) {
      line = br.readLine()
      strs = line.split("\\t")
      word_no += (strs(0).toInt -> strs(1))
    }
    //最后是特殊类别对应的名称
    for (i <- 0 until named_tag) {
      line = br.readLine()
      strs = line.split("\\t")
      index_tag += (strs(0).toInt -> strs(1))
    }
    //校验一下是不是完全读完了
    line = br.readLine()
    if (line != null)
      println("读入时出错！")
    else
      println("加载模型成功")
    val vec_arr = nkv.map(_.length).distinct
    if (vec_arr.length == 1)
      total_classes = vec_arr.head
    println(nkv.length, nkv.map(_.length).distinct.map(_.toString).reduce(_ + "," + _))
    println(word_no.size, word_no.take(5).map(_.toString()).reduce(_ + "\t" + _))
    index_tag.foreach(println)
    //生成这个word_vec
    for (line <- nkv.zipWithIndex) {
      word_vec += (word_no(line._2) -> s_Vectors.dense(line._1.map(_.toDouble)))
    }
    println(word_vec.size)


    val tmr_end = System.currentTimeMillis()
    println(s"载入模型，耗时${(tmr_end - tmr_start) / 1000.0}s")
  }

  /**
    * 在计算中，如果是不存在的词，那就直接忽略掉，不对存在的词进行稀释
    * 同时，应当返回判断中对主题贡献最大的词
    *
    * @param words
    * @return
    */
  def judge(words: Array[String], get_top_words: Boolean): (Array[String], Array[(String, Double)]) = {
    var sum_arr = (for (i <- 0 until total_classes) yield 0.0).toArray
    var count = 0
    for (word <- words)
      if (word_vec.contains(word)) {
        count += 1
        val origin_word_vec = word_vec(word).toArray
        val normalize_word_vec = origin_word_vec.map(ele => ele / origin_word_vec.sum)
        sum_arr = sum_arr.zip(normalize_word_vec).map(ele => ele._1 + ele._2)
      }
    sum_arr = sum_arr.map(_ / count)
    var tag_judge = Array[String]()
    for (i <- 0 until total_classes)
      if (sum_arr(i) > thershold)
        tag_judge = tag_judge :+ (if (index_tag.contains(i)) index_tag(i) else i.toString)
    tag_judge = tag_judge.distinct
    var top_words = Array[(String, Double)]()
    //把对主题贡献最大的词统计出来
    if (get_top_words) {
      var words_score = Map[String, Double]()
      val tag_index = index_tag.values.zip(index_tag.keys).toMap
      for (word <- words.filter(ele => word_vec.contains(ele))) {
        var score = 0.0
        val vec = word_vec(word).toArray
        for (topic <- tag_judge.map(ele => if (tag_index.contains(ele)) tag_index(ele) else ele.toInt))
          if (vec(topic) > score)
            score = vec(topic) //记录贡献最高的得分
        if (words_score.contains(word))
          words_score += (word -> (score + words_score(word)))
        else
          words_score += (word -> score)
      }
      top_words = words_score.toArray.sortWith((e1, e2) => e1._2 > e2._2)
    }
    (tag_judge, top_words)
  }


}
