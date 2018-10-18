package SituationAwareness_2_0a.RebuildLightLDAModel

import java.io.{File, FileInputStream}
import java.util.Properties

import SituationAwareness2.CudeHDFS._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object MajorMathod {
  val hu = new HDFSUtils
  var hdfs_ip = ""
  var seg_files_path = ""
  var spark_master = ""
  var spark_app_name = ""
  var spark_max_retry = ""
  var lda_result = ""
  var lda_specail_class = ""
  var lda_partitions = 10
  var lda_minDF = 1
  var lda_minTF = 1
  var lda_vocab_size = 1
  var lda_K = 10
  var lda_alpha = 0.01
  var lda_beta = 0.1
  var lda_mh_steps = 2
  var lda_iter_times = 20
  var lda_rdd_path = ""
  var lda_model_path = ""

  var spark: SparkSession = null
  var doc: Documents = null

  def load_Properties(): Unit = {
    val properties = new Properties()
    val path = new File(new File(".").getAbsolutePath + "/conf/troublesome.properties").getAbsolutePath //这个是用在服务器上的？
    println(path)
    properties.load(new FileInputStream(path))
    hdfs_ip = properties.getProperty("hgh.hdfs.ip")
    seg_files_path = properties.getProperty("hgh.hdfs.seg_files")
    spark_master = properties.getProperty("hgh.spark.master_domain")
    spark_app_name = properties.getProperty("hgh.spark.app_name")
    spark_max_retry = properties.getProperty("hgh.spark.max_retries")
    lda_result = properties.getProperty("hgh.light_lda.result_path")
    lda_specail_class = properties.getProperty("hgh.light_lda.self_def_class")
    lda_partitions = properties.getProperty("hgh.light_lda.partitions").toInt
    lda_minDF = properties.getProperty("hgh.light_lda.min_DF").toInt
    lda_minTF = properties.getProperty("hgh.light_lda.min_TF").toInt
    lda_vocab_size = properties.getProperty("hgh.light_lda.vocab_size").toInt
    lda_K = properties.getProperty("hgh.light_lda.K").toInt
    lda_alpha = properties.getProperty("hgh.light_lda.alpha").toDouble
    lda_beta = properties.getProperty("hgh.light_lda.beta").toDouble
    lda_mh_steps = properties.getProperty("hgh.light_lda.mh_steps").toInt
    lda_iter_times = properties.getProperty("hgh.light_lda.iter_times").toInt
    lda_rdd_path = properties.getProperty("hgh.light_lda.save_rdd")
    lda_model_path = properties.getProperty("hgh.light_lda.model_path")

    hu.init(hdfs_ip)
    val arr = hu.list_hdfs_file(hdfs_ip + seg_files_path)
    arr.foreach(println)
  }

  def spark_init(): Unit = {
    val spark_conf = new SparkConf()
    //      .setMaster(spark_master)
    //      .setAppName(spark_app_name)
    //      .set("spark.port.maxRetries", spark_max_retry)
    spark = SparkSession.builder().config(spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("warn")
  }

  /**
    * 会将在seg_files_path里面的所有文件都加载，作为数据
    */
  def load_dataframe(): Unit = {
    var res_arr = Array[DataFrame]()
    var total = 0L
    for (file <- hu.list_hdfs_file(hdfs_ip + seg_files_path)) {
      val df = spark.read
        .options(Map("header" -> "false", "delimiter" -> "\t"))
        .csv(file)
        .toDF("file_name", "title", "sentence")
      df.show(5)
      res_arr = res_arr :+ df
      total += df.count()
    }
    val res = res_arr.reduce((ele1, ele2) => ele1.union(ele2))
    res.persist(StorageLevel.MEMORY_AND_DISK)
    res.show(5)
    //开始准备读documents
    doc = new Documents
    doc.lda_minDF = lda_minDF
    doc.lda_minTF = lda_minTF
    doc.lda_vocab_size = lda_vocab_size
    val br = hu.open_br(hdfs_ip + lda_result + lda_specail_class)
    doc.load_special_class(br)
    doc.trans_df_2_rdd(res, "sentence", lda_partitions, total)

  }

  /**
    * 训练并保存模型
    */
  def fit_model(): Unit = {
    val model = new LightLDAModel(spark)
      .set_K(lda_K)
      .set_alpha(lda_alpha)
      .set_beta(lda_beta)
      .set_mh_steps(lda_mh_steps)
      .set_iter_time(lda_iter_times)
    model.init(doc)
    model.fit()

    hu.delete_dir(hdfs_ip + lda_result + lda_rdd_path) //如果以及有模型，那就先全删掉
    val bw = hu.open_bw(hdfs_ip + lda_result + lda_model_path)
    model.save(bw, hdfs_ip + lda_result + lda_rdd_path)
  }

  def main(args: Array[String]): Unit = {
    load_Properties()
    spark_init()
    load_dataframe()
    fit_model()
  }
}
