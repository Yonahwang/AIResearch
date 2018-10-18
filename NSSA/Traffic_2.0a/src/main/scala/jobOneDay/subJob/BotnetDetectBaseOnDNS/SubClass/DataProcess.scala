package jobOneDay.subJob.BotnetDetectBaseOnDNS.SubClass

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by wzd on 2018/1/17.
  */
class DataProcess extends loggerSet{

  def main(oriData:DataFrame, spark:SparkSession, properties:Properties) = {
    //获取恶意域名检测结果
    val maliciousDomain = getMaliciousDomain(spark, properties)
    //关联出有访问过恶意域名的IP
    val requestIP = getRequestIP(oriData, maliciousDomain, spark, properties)

    //获取时间标签
//    val result = getTime(requestIP)
    requestIP
  }

  //获取恶意域名名单
  def getMaliciousDomain(spark: SparkSession, properties: Properties) = {
    import spark.implicits._
    val path = properties.getProperty("malicious.domain")
    val oridomain = spark.sparkContext.textFile(path).toDF("name")
//    //分割数据
//    val domain = oridomain.map {
//      line =>
//        val temp = line.split("\t")
//        (temp(0), temp(1))
//    }.toDF("name", "label")
//    logger.error("获取恶意域名名单>>>")
//    domain
    oridomain
  }

  //关联访问过恶意域名的IP
  def getRequestIP(dnsdata: DataFrame, domaindata: DataFrame, spark: SparkSession, properties: Properties) = {
    //获取恶意域名
    val domain: Array[String] = domaindata.rdd.map(_.getAs[String]("name")).collect()
//    println("malicious domain:",domain.toList)
    //筛选出只访问恶意域名的dns记录
    val result = dnsdata.filter {
      line =>
        domain.contains(line.getAs[String]("queries_name"))
    }.toDF().select("srcip", "queries_name", "answers_address","timestamp")
    logger.error("过滤恶意域名IP>>>")
    result
  }

  def getTime(data:DataFrame) = {
    //flow_id
    val addTime = org.apache.spark.sql.functions.udf((str: String) => {
      val result = if(str == null || str == "") 1518059314.toLong * 1000 else str.split("_")(0).toLong * 1000
      result
    })
    val result = data.withColumn("Date", addTime(data("flow_id")))
    result
  }


}
