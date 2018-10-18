package jobOneHour.subJob.dnsCovDetect

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Matrices, Matrix}
import org.apache.spark.rdd.RDD

import scala.collection.Map

class MyMulticlassMetrics(predictionAndLabels: RDD[(Double, Double)])
  extends MulticlassMetrics(predictionAndLabels: RDD[(Double, Double)]) {

  private lazy val labelCountByClass: Map[Double, Long] = predictionAndLabels.values.countByValue()
  private lazy val labelCount: Long = labelCountByClass.values.sum
  private lazy val tpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (label, if (label == prediction) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val fpByClass: Map[Double, Int] = predictionAndLabels
    .map { case (prediction, label) =>
      (prediction, if (prediction != label) 1 else 0)
    }.reduceByKey(_ + _)
    .collectAsMap()
  private lazy val confusions = predictionAndLabels
    .map { case (prediction, label) =>
      ((label, prediction), 1)
    }.reduceByKey(_ + _)
    .collectAsMap()

  def printEverything() = {

    println("labelCountByClass", labelCountByClass)
    println("labelCount", labelCount)
    println("tpByClass", tpByClass)
    println("fpByClass", fpByClass)
    println("confusions(label, prediction)", confusions)
    println("confusionMatrix, predicted classes are in columns" )
    println(confusionMatrix)
    println("precision",accuracy)
//    println("",)

    def confusionMatrix2: Matrix = {
      val n = 2
      val values = Array.ofDim[Double](n,n)
      var i = 0
      while (i < n) {
        var j = 0
        while (j < n) {
          values(i)(j) = confusions.getOrElse((labels(i), labels(j)), 0).toDouble
//          println((i,j),values(i)(j))
          j += 1
        }
        i += 1
      }
//      println(values.flatten.toVector)
      Matrices.dense(n,n,values.flatten).transpose
    }

    println(confusionMatrix2)
  }
}



