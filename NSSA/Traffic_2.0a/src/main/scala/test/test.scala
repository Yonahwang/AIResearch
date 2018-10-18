package test

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try


/**
  * Created by Administrator on 2018/6/11.
  */
object test {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setMaster("local").setAppName("test")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    import spark.implicits._
    val test = spark.sparkContext.parallelize(List((1,2,3),(3,4,5))).toDF("a","b","c")
    test.show(5, false)

    println(System.getProperty("user.dir"))

    val dataFrame = spark.createDataFrame(Seq(
      (1, "example1", "a|b|c"),
      (2, "example2", "d|e")
    )).toDF("id", "name", "content")


  }

}
