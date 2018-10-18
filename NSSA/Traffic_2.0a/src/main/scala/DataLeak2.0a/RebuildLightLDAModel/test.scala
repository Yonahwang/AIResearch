package SituationAwareness_2_0a.RebuildLightLDAModel

import java.io._

import breeze.io.TextReader.InputStreamReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

object test {
  val search_dir = "D:\\ProjectResources\\resources_new\\UsingSpark\\segged_data"

  def get_local_df(spark: SparkSession): (DataFrame, Long) = {
    val dir = new File(search_dir)
    var res_arr = Array[DataFrame]()
    var total = 0L
    if (dir.isDirectory) {
      val fl = dir.listFiles().take(10)
      var k = 0

      for (file <- fl) {
        k += 1
        if (k % 10 == 0)
          println(k)
        //        println(file)
        val df = spark.read
          .options(Map("header" -> "false", "delimiter" -> "\t"))
          .csv(file.getAbsolutePath)
          .toDF("file_name", "sentence")
        //        println(df.count())
        res_arr = res_arr :+ df
        total += df.count()
      }
    }
    val res = res_arr.reduce((ele1, ele2) => ele1.union(ele2))
    res.persist(StorageLevel.MEMORY_AND_DISK)
    res.show(5)
    println(total)
    println(res.rdd.getNumPartitions)
    (res, total)
  }

  def get_documents(df: DataFrame, total: Long): Documents = {
    val d = new Documents
    d.lda_minDF = 5
    d.lda_minTF = 2
    d.lda_vocab_size = 100000
    d.trans_df_2_rdd(df, "sentence", 20, total)
    d
  }

  /**
    * 迭代200次，采样率5%，耗时289s
    * 迭代10次，采样率100%，耗时37s
    * 看上去，后者的区分能力更好
    *
    * @param spark
    * @param doc
    */
  def fit_model(spark: SparkSession, doc: Documents): Unit = {
    val model = new LightLDAModel(spark)
      .set_K(20)
      .set_alpha(0.01)
      .set_beta(0.1)
      .set_mh_steps(2)
      .set_iter_time(30)
      .set_sub_sample(1)
    model.init(doc)
    model.fit()
    //写一个到本地吧。
    val local_path = "C:\\Users\\Makigumo\\IdeaProjects\\cleaner_project\\src\\main\\resources\\LightLDA\\model"
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(local_path))))
    model.save(bw, "")
  }

  def load_model(spark: SparkSession): Unit = {
    val local_path = "C:\\Users\\Makigumo\\IdeaProjects\\cleaner_project\\src\\main\\resources\\LightLDA\\light_lda.model"
    var br = new BufferedReader(new FileReader(new File(local_path)))
    val model = new LightLDAModel(spark)
    model.load_old_model(br)
  }

  def main(args: Array[String]): Unit = {
    val spconf = new SparkConf()
      .setMaster("local")
      .setAppName("SOC_Mllib")
      .set("spark.port.maxRetries", "100")
    val spark = SparkSession.builder().config(spconf).getOrCreate()
    //
    var rdd = spark.sparkContext.parallelize(
      (for (i <- 0 until 50) yield i).toSeq)
    rdd = rdd.repartition(10)
    println("parts:" + rdd.getNumPartitions)
    println("count:" + rdd.count())
    val sub_rdd = rdd.sample(false, 0.5, new Random().nextLong())
    sub_rdd.foreach(println)
    println("parts:" + sub_rdd.getNumPartitions)
    println(s"count:${sub_rdd.count()}")
    //
    //    val (df, total) = get_local_df(spark)
    //    val doc = get_documents(df, total)
    //    fit_model(spark, doc)
    //
    load_model(spark)
  }
}
