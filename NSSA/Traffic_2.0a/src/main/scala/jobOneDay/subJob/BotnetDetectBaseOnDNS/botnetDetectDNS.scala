package jobOneDay.subJob.BotnetDetectBaseOnDNS

import java.io.{BufferedInputStream, File, FileInputStream, Serializable}
import java.util.Properties

import jobOneDay.subClass.saveToKAFKA
import jobOneDay.subJob.BotnetDetectBaseOnDNS.SubClass._
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Try

/**
  * Created by Administrator on 2018/6/14.
  */
class botnetDetectDNS(spark: SparkSession, properties: Properties) extends Serializable with loggerSet with saveMethod with readMethod with saveToKAFKA{


  def main() = {

    //log4j日志
    Logger.getLogger("org").setLevel(Level.ERROR)//显示的日志级别
    var directory = new File("..")
    PropertyConfigurator.configure(directory + "/conf/botnetBaseOnDNSlog4j.properties")

    //配置spark环境
    val sql: SQLContext = spark.sqlContext
    logger.error("spark.sparkContext.defaultParallelism:" + spark.sparkContext.defaultParallelism)
    logger.error("spark.sparkContext.defaultMinPartitions:" + spark.sparkContext.defaultMinPartitions)

    //读取HDFS文件
    val oriData = readDataFromHDFS(spark, properties)

    //数据处理//时间字段后面要修改回来
    val dataClass = new DataProcess()
    val dataProcessResult = dataClass.main(oriData, spark, properties)
    //重分区
    val dataProcessRepartResult = dataProcessResult.repartition(spark.sparkContext.defaultParallelism)
    dataProcessRepartResult.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //计算群体
    val clusterModel = new Cluster()
    val clusterResult = clusterModel.main(dataProcessRepartResult, spark)
    val labelDomainMap = clusterResult._1
    val ipLabeMap = clusterResult._2
    //重分区

    //计算周期性
    val periodClass = new Period()
    val periodRe = periodClass.main(dataProcessRepartResult, labelDomainMap, ipLabeMap, spark)
    //重分区
    val periodResult = periodRe.repartition(periodRe.rdd.partitions.size)

    //计算僵尸网络
    val calculateBot = new BotNet()
    val botnetR = calculateBot.main(labelDomainMap, ipLabeMap, periodResult, spark, properties)
    val botnetResult = botnetR.repartition(periodRe.rdd.partitions.size)


    //过滤结果
    val labelS = botnetResult.rdd.map{x => (x.getAs[String]("label"),x.getAs[String]("field03"))}.distinct.collect()
    val k4 = properties.getProperty("botnet.k4")
    val resultF = botnetResult.filter{x => x.getAs[String]("content_confusion").toDouble > k4.toDouble}
    val botnetResultCount = botnetResult.count()
    val resultCount = resultF.count().toInt
    logger.error(s"结果过滤前记录数： $botnetResultCount")
    if(resultCount > 1){
      logger.error(s"僵尸网络结果记录数为 $resultCount")
      //保存到ES(模型结果表)(tmodelresult)
      saveToES(botnetResult, properties)
      //保存到kafka
            toKafka(spark, (properties.getProperty("kafka.nodes"),properties.getProperty("kafka.topic3")), botnetResult)
      //保存到postgre
//      saveToPostgreSQLLine(botnetResult, properties, spark)
      //保存到本地
      saveToFile(botnetResult, properties.getProperty("botnet.result.local"), "botnetBaseOnDNS.csv")
    }
    else{
      logger.error("僵尸网络结果数为 0 ，没有发现僵尸网络")
    }

  }


  def getSparkSession(properties: Properties): SparkSession = {
    val sparkconf = new SparkConf()
      .setMaster(properties.getProperty("spark.master.url"))
      .setAppName(properties.getProperty("spark.app.name"))
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("spark.default.parallelism", properties.getProperty("spark.default.parallelism"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }



}
