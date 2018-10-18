package jobOneHour.subJob.dnsCovDetect

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object replaceIP {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    //    配置spark
    val conf = new SparkConf().
      setMaster("local").
      setAppName("fakeID")

    //    新建spark对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext


//    val files: Array[File] = new File("D:\\DNScovertTunnelSample\\").listFiles()
//    files.foreach(println)

    val IPS: List[String] = sc.textFile("F:\\IPCandidates.txt").collect().toList
//    IPS.foreach(println)


//    val dfs: Array[DataFrame] = files.map{
//      filename =>{
//        val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> filename.toString)
//        val data = spark.read.format("com.databricks.spark.csv").options(options).load()
//        data.show()
//        data.printSchema()
//        println(data.count())
//        data
//      }
//    }
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "hdfs://172.16.14.31:9000/spark/data/dns/201805311515.txt")
    val data = spark.read.format("com.databricks.spark.csv").options(options).load()
    println(data.count())
//    val data = spark.read.json("hdfs://172.16.14.31:9000/usr/hadoop/nta/dns/dns201805311100.txt")
//    val generateIpUdf = udf(() =>{
//      val index = scala.util.Random.nextInt(20)
////      println(index)
//      val IP = IPS(index)
//      IP
//    } )
//    val generateTimestampUdf = udf(() =>{
//      val index = scala.util.Random.nextInt(7200)
//      val ts = 1527476400 + index
//      ts
//    } )
//    val dateIpTime = data.drop("timestamp")
//      .drop("srcip")
//      .withColumn("srcip",generateIpUdf())
//      .withColumn("timestamp",generateTimestampUdf())
////    dateIpTime.write.json("F:\\dnsCoverChannelJson.txt")
//    dateIpTime.show()
  }
}
