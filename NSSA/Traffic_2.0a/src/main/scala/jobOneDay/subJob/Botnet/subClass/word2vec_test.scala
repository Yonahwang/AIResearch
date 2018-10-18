package jobOneDay.subJob.Botnet.subClass

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by Administrator on 2018/3/20.
  */
object word2vec_test {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word2Vec example").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = sqlContext.createDataFrame(Seq(
      "Who am i".split(" "),
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    documentDF.show(false)



    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(6)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    result.select("result").take(3).foreach(println)

    val vecs = model.getVectors
    vecs.show(false)


    println("sum:"+(0.018192+0.0700136-0.016833))
    println("mean:"+(0.018192+0.0700136-0.016833)/3)
  }




}
