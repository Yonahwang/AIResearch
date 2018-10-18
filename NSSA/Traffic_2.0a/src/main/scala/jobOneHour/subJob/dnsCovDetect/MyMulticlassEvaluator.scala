package jobOneHour.subJob.dnsCovDetect

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Dataset, Row, SparkSession}


class MyMulticlassEvaluator extends MulticlassClassificationEvaluator {

  /**
    * param for metric name in evaluation (supports `"f1"` (default), `"weightedPrecision"`,
    * `"weightedRecall"`, `"accuracy"`)
    *
    * @group param
    */

  var is_new_metric = false

  override def setMetricName(value: String): this.type = {
    val metricList = Array("f1", "weightedPrecision", "weightedRecall", "accuracy")

    if (metricList contains value)
      set(metricName, value)
    else {
      this.is_new_metric = true
      this
    }
  }

  //  override def evaluate(): Double = {
  override def evaluate(dataset: Dataset[_]) = {
    val schema = dataset.schema

    val predictionAndLabels =
      dataset.select(col($(predictionCol)), col($(labelCol)).cast(DoubleType)).rdd.map {
        case Row(prediction: Double, label: Double) => (prediction, label)
      }
    val metrics = new MulticlassMetrics(predictionAndLabels)



    val metric =
      if (is_new_metric)
        $(metricName) match{
          case "weightedFalsePositiveRate" => - metrics.weightedFalsePositiveRate
        }

      else
        $(metricName) match {
          case "f1" => metrics.weightedFMeasure
          case "weightedPrecision" => metrics.weightedPrecision
          case "weightedRecall" => metrics.weightedRecall
          case "accuracy" => metrics.accuracy
        }
    metric
  }



}

case class LP(label:Double, pre:Double)

object test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("metricTest")
    //    新建spark对象
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    val data = Seq(LP(1.0,1.0),LP(1,0),LP(1,0),LP(0,0),LP(0,0),LP(0,0),LP(0,0),LP(0,1),LP(0,1),LP(0,1)).toDS()
//    data.printSchema()
    val a = new MyMulticlassEvaluator()
      .setLabelCol("label")
      .setPredictionCol("pre")
//      .setMetricName("accuracy")
      .setMetricName("f1")
      .evaluate(data)



    val dataRDD = data.rdd.map{
      case LP(label, pre) =>
        (pre, label)
    }

    val ru = scala.reflect.runtime.universe
//    val m = ru.runtimeMirror(getClass.getClassLoader)
//    val im = m.reflect(new MulticlassMetrics(dataRDD))
//    val con = im.getter(confusions)

//    val labelCountByClass = new MulticlassMetrics(dataRDD).getter(confusions)

    val b = new MyMulticlassMetrics(dataRDD)
    b.printEverything()





  }
}

