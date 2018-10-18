package SituationAwareness_2_0a.RebuildLightLDAModel

import java.io._

import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.linalg.{SparseVector => SPSV}
import org.apache.spark.mllib.linalg.{Vector => SPV, Vectors => SPVs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * 用来存所有文档的数据结构
  *
  * 2018-6-21改进：原本每个词都单独存放。但对于相同的词而言，先改变一个的主题，对于后者的采样变化约等于没有。
  * -   并且，作为一个迭代算法而言，这个近似是可以接受的
  * 2018-6-22debug：上面的那个改进，是假的。。。。
  * 2018-6-25思考：一个词从属于特定的类别？以及一个不参与训练的分类？
  * -   如果一个词从属于多个类别，但其中包括了不参与训练的分类，这要如何处理？
  * -   暂定方案：在k类之外，还有untrain_K个类别。这些untrain_K个类别，仅有从属于该类别的词，有可能被分配进去？
  * -       这个方案似乎不太行。为了几个词，多构造了也许很多个分类。为nkv、ndk带来不小的负担。
  * -   这么决定好了：一个词，如果属于不训练的类别，那么就直接把词替换为<PAD>。
  * 2018-6-27改进：增加了不训练分类的设定，并且统一了读取入口
  */
class Documents {
  var docs: RDD[(Int, Array[Int])] = null
  var word_index = Map[String, Int]()
  var index_word = Map[Int, String]()
  var special_class = Map[Int, Array[Int]]()
  var index_tag = Map[Int, String]()
  var untrain_class = Map[String, Array[String]]()

  var lda_minDF = 1
  var lda_minTF = 1
  var lda_vocab_size = 1

  var use_expert_info = false //是否使用额外的类别信息
  var cache_expert_info = Array[(String, Boolean, Array[String])]()

  def save_word_index(fw: FileWriter): Unit = {
    for (line <- index_word)
      fw.write(line._1.toString + "\t" + line._2 + "\r\n")
    fw.close()
  }

  /**
    * 将dataframe转换为rdd的便捷函数。这个dataframe必须包含一列String类型，该列代表这已经分词并使用空格隔开的词
    *
    * @param input_df
    * @param sentence_col
    * @param partitions
    */
  def trans_df_2_rdd(input_df: DataFrame,
                     sentence_col: String,
                     partitions: Int,
                     total_lines: Long): Unit = {
    try {
      val untrain_words = cache_expert_info.filter(!_._2).flatMap(_._3) //设置不需要被训练的词。
      val split_udf = org.apache.spark.sql.functions.udf {
        sentence: String =>
          val reg = "[0-9]+".r
          var chn_words = Array[String]()
          try {
            if (sentence != null)
              chn_words = sentence.split(" ")
                .filter(ele => ele.length > 1 &&
                  reg.replaceAllIn(ele, "") != "" &&
                  !untrain_words.contains(ele)) // && ele.length != ele.getBytes().length)
          }
          catch {
            case e: Exception =>
              println("出错的句子：" + sentence)
              e.printStackTrace()
          }

          chn_words
      }
      var df = input_df.withColumn("words", split_udf(input_df.col(sentence_col)))
      df.persist(StorageLevel.MEMORY_AND_DISK)
      //      println(lda_minTF,lda_minDF,lda_vocab_size)
      val cvModel = new CountVectorizer()
        .setInputCol("words")
        .setOutputCol("features")
        .setVocabSize(lda_vocab_size) //最大词的数量
        .setMinDF(lda_minDF) //最少在多少片文本中出现
        .setMinTF(lda_minTF) //单篇文本中出现次数
        .fit(df)
      val vocab = cvModel.vocabulary
      val index2word = (for (i <- 0 until vocab.length) yield i).zip(vocab).toMap
      //      index2word.take(5).foreach(println)
      df = cvModel.transform(df)
      df.unpersist()
      df.show(5)
      val ident_words_udf = org.apache.spark.sql.functions.udf {
        (words: SPSV) =>
          words.toArray.map(_.toInt)
            .zipWithIndex.filter(_._1 != 0).flatMap(line => for (i <- 0 until line._1) yield line._2).toSeq //让前面是词的序号，后面是词的数量
      }
      df = df.withColumn("ident_words", ident_words_udf(df.col("features")))
      df.printSchema()
      df.show(5)
      val balanced_parts = math.max(partitions, total_lines / 10000).toInt
      docs = df.rdd.map(_.getAs[Seq[Int]]("ident_words")).zipWithIndex()
        .map(line => (line._2.toInt, line._1.toArray))
        .repartition(balanced_parts)
      docs.persist(StorageLevel.MEMORY_AND_DISK)
      //      docs.take(5).foreach {
      //        line =>
      //          println(line._1)
      //          println(line._2.map(_.toString()).reduce(_ + " " + _))
      //      }
      index_word = index2word
      word_index = index2word.values.zip(index2word.keys).toMap
      println("词对应的编号：")
      word_index.take(5).foreach(println)
      println(word_index.size)
      get_specail_class()
      println("success")
    }
    catch {
      case e: Exception =>
        println("failed")
        e.printStackTrace()
    }
  }

  /**
    * 重写了读入的方式。
    * 现在敲定的数据格式是这样子的：
    * 类别的名称，是否进行训练（0或1），类别中包含的词
    * 在读入docs之前调用本函数
    *
    * @param br
    */
  def load_special_class(br: BufferedReader): Unit = {
    use_expert_info = true
    var line = br.readLine()
    while (line != null) {
      val strs = line.toLowerCase.split("\\t")
      if (strs.length == 3) {
        val class_name = strs(0)
        val train_or_not = if (strs(1) == "1") true else false
        val words = strs(2).split(",")
        cache_expert_info = cache_expert_info :+ ((class_name, train_or_not, words))
      }
      line = br.readLine()
    }
    br.close()
  }

  def get_specail_class(): Unit = {
    val word_class = cache_expert_info.filter(_._2).flatMap(line => line._3.map(ele => (ele, line._1)))
      .filter(ele => word_index.contains(ele._1))
    val class_index = word_class.map(_._2).distinct.zipWithIndex.toMap
    index_tag = class_index.values.zip(class_index.keys).toMap
    special_class = word_class.groupBy(_._1)
      .map(line => (word_index(line._1), line._2.map(ele => class_index(ele._2))))
    println("出现的自定义类的词：")
    word_class.foreach(println)
  }


}
