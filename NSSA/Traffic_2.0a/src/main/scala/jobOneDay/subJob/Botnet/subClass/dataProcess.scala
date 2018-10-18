package jobOneDay.subJob.Botnet.subClass

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by wzd on 2018/1/17.
  */
class dataProcess extends loggerSet{

  def main(oriData:DataFrame, spark:SparkSession, properties:Properties) = {
    //获取恶意域名检测结果
    val maliciousDomain = getMaliciousDomain(spark, properties)
    //关联出有访问过恶意域名的IP
    val requestIP = getRequestIP(oriData, maliciousDomain, spark, properties)
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
    //筛选出只访问恶意域名的dns记录
    val result = dnsdata.filter {
      line =>
        domain.contains(line.getAs[String]("queries_name"))
    }.toDF()
    logger.error("过滤恶意域名IP>>>")
    result
  }

}
