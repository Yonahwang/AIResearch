package jobOneDay.subJob.BotnetDetectBaseOnDNS.SubClass

import java.text.SimpleDateFormat

import breeze.linalg.DenseVector
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession, _}



/**
  * Created by Administrator on 2018/2/2.
  */


case class ipDomain(srcip:String, domain:String, vec:Vector)
case class periodR(srcip:String, domain:String, rk:Double, r0:Double, ak:Double, vec:Vector, vecName:String)

class Period extends loggerSet{

  def main(data:DataFrame,labelDomainMap: Map[String, Map[String, (Double, Double)]], ipLabeMap: Map[String, String],spark:SparkSession)={
    logger.error("period >>> begin")
    //添加时间标签
    val dataSplit = splitData(data, ipLabeMap)
    //整合出类别-ip-domain name的key对，并统计出时间间隔的vector
    val featureVec: (Dataset[ipDomain], List[String]) = hashSeq(dataSplit, labelDomainMap, ipLabeMap, spark)
    //计算每个key的时间周期性
    val periodResult = calculatePerid(featureVec._1, featureVec._2, spark)
    logger.error("period >>> end")
    periodResult
  }

  //添加时间标签
  def splitData(data:DataFrame, ipLabeMap: Map[String, String])={
    //过滤数据
    val ip = ipLabeMap.keySet.toList
    val filterData = data.filter(line => ip.contains(line.getAs[String]("srcip"))).repartition(data.rdd.partitions.size).toDF()
    //打上时间标签，10分钟一个标签
    val addTime = org.apache.spark.sql.functions.udf((time: String) => {
      var result = ""
      if(time == "" || time == null){
        result = "00:00"
      }
      else{
        val time_head = new SimpleDateFormat("HH:").format(time.toLong * 1000)
        val min: String = new SimpleDateFormat("mm").format(time.toLong * 1000)
        result = time_head + min.substring(0,1) + "0"

      }
      result
    })
    filterData.withColumn("interval", addTime(filterData("timestamp")))
//    val addTime = org.apache.spark.sql.functions.udf((time: Long) => {
//      val time_head = new SimpleDateFormat("HH:").format(time)
//      val min: String = new SimpleDateFormat("mm").format(time)
//      time_head + min.substring(0,1) + "0"
//    })
//    filterData.withColumn("interval", addTime(filterData("Date")))
  }


  //整合出类别-ip-domain name的key对，并统计出时间间隔的vector
  def hashSeq(data:DataFrame, labelDomainMap: Map[String, Map[String, (Double, Double)]], ipLabeMap: Map[String, String], spark:SparkSession)={
    import spark.implicits._
    //过滤原始数据
    //ipLabeMap:Map(ip, class)
    //labelDomainMap:Map(类别, 特征名称, 聚类得分)
    val labelDomainMapBroadcast = spark.sparkContext.broadcast(labelDomainMap)
    val ipLabeMapBroadcast = spark.sparkContext.broadcast(ipLabeMap)
    //broadcast还没用
    val dataFilter = data.filter{
      line =>
        val ip = line.getAs[String]("srcip")
        val domain = line.getAs[String]("queries_name")
        val domainSet = labelDomainMap(ipLabeMap(ip)).keySet
        ipLabeMap.keySet.contains(ip) && domainSet.contains(domain)
    }.select("srcip","queries_name","interval").distinct().repartition(data.rdd.partitions.size)


    //构建时间序列特征向量
    //使用one-hot编码，再统计所有相同key的vector
    val indexerProcess = new StringIndexer().setInputCol("interval").setOutputCol("intervalIndex").fit(dataFilter)
    //one hot 处理
    val onehostProcess = new OneHotEncoder().setInputCol(indexerProcess.getOutputCol).setOutputCol("intervalOnehot")
    //转换成向量形式
    val vectorProcess = new VectorAssembler().setInputCols(Array(onehostProcess.getOutputCol)).setOutputCol("intervalOnehotVector")
    //使用pipeline管道进行模型拟合
    val pipelineStage = Array(indexerProcess, onehostProcess, vectorProcess)
    val pipline = new Pipeline().setStages(pipelineStage)
    val pipModel = pipline.fit(dataFilter)
    val pipModel1DF = pipModel.transform(dataFilter)
    //获取index对应的域名
    val intervalTimeName = indexerProcess.labels.toList

    //转换为case class类型
    val dataTransferCaseclass = pipModel1DF.map{
      line =>
        ipDomain(line.getAs[String]("srcip"), line.getAs[String]("queries_name"), line.getAs[Vector]("intervalOnehotVector"))
    }
    //构建类别-ip-domain name对（reduceByKey），并将相同key的vector统计成一条
    val keyData = dataTransferCaseclass.rdd.keyBy(k => (k.srcip,k.domain)).reduceByKey{
      (x, y) => {
        val xVec = x.vec
        val yVec = y.vec
        val vec = DenseVector(xVec.toArray) + DenseVector(yVec.toArray)
        ipDomain(x.srcip, x.domain, Vectors.dense(vec.toArray))
      }
    }.map(x=> x._2).toDS()
    logger.error("period >>> key data")
    (keyData,intervalTimeName)
  }


  //计算周期（计算每一个周期）
  def calculatePerid(featureVec: Dataset[ipDomain], intervalTimeName:List[String],spark:SparkSession)={
    //featureVec: ipDomain(srcip:String, domain:String, vec:Vector)
    import spark.implicits._
    val k = 1
//    val kMax = 144/4
//    val intervalNum = 144
    val intervalNum = featureVec.take(1).head.vec.toArray.length
    val timeName = intervalTimeName.slice(0,intervalTimeName.length-1).mkString("#")
    //对每个ip-domain key进行周期计算，每个key只有一个得分结果
    //vec长度为144
    //计算周期得分，返回(ip, domain, rk, r0, ak)
    val result = featureVec.rdd.mapPartitions{
      part =>
        part.map{
          line =>
            var rk = 0.0
            val vec = line.vec.toArray
            //计算r(0)
            val r0 = vec.map(x=>x*x).sum
            //计算r(k)
            for(i <- 0 to (intervalNum -1 - k)){
              rk = rk +  vec(i) * vec(i + k)
            }
            //计算α(k)
            val ak = rk/r0
            periodR(line.srcip, line.domain, rk, r0, ak, line.vec, timeName)
        }
    }.toDS
    logger.error("period >>> rk")
//    result.show(5, false)
    result
  }



}
