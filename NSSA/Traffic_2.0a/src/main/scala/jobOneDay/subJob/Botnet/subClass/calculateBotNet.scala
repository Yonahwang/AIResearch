package jobOneDay.subJob.Botnet.subClass

import java.io.Serializable
import java.util.{Properties, UUID}

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by wzd on 2018/1/17.
  */

class calculateBotNet extends Serializable with loggerSet{

  def main(requestIP:DataFrame, spark:SparkSession, properties:Properties) = {
    //计算僵尸主机概率
    val botProbability = calculateBotProbability(requestIP, spark, properties)
    //寻找僵尸网络
    val botnetProbability = searchBotnet(botProbability, spark, properties)
    //关联历史数据
//    val botnetProbabilityConnectHistory = connetctHistory(botnetProbability, requestIP, spark, properties)
    val botnetProbabilityConnectHistory = connHistory(botnetProbability, requestIP, spark, properties)
    botnetProbabilityConnectHistory
  }


  //计算僵尸主机的概率
  def calculateBotProbability(data: DataFrame, spark: SparkSession, properties: Properties) = {
    data.createOrReplaceTempView("inputdata")
    //计算僵尸主机概率并替换了空值
    val result = spark.sql(
      """select distinct
        |       case when a.srcip = 'null' then '0.0.0.0' else a.srcip end as srcip,
        |       case when a.queries_name = 'null' then 'www' else a.queries_name end as queries_name,
        |       cast(b.probability as string)
        |from inputdata a
        |left join (select srcip, sum(sum_code)/count(sum_code) as probability
        |           from (select distinct srcip, queries_name,
        |                        case when flags_reply_code = '3' then 1 else 0 end as sum_code
        |                 from inputdata)
        |           group by srcip) b
        |on a.srcip = b.srcip
      """.stripMargin)
    logger.error("计算僵尸主机概率>>>")
    result.filter("srcip != '0.0.0.0'").toDF
  }

  //寻找僵尸网络
  def searchBotnet(data: DataFrame, spark: SparkSession, properties: Properties) = {
    //选择需要字段
    val addcol = org.apache.spark.sql.functions.udf((str: String) => {
      1.0
    })
    val selectDataVisit = data.withColumn("visit", addcol(data("queries_name")))
    //建立索引
    val indexer1 = new StringIndexer().setInputCol("srcip").setOutputCol("srcip_index").fit(selectDataVisit)
    //输出double类型
    val indexer1data = indexer1.transform(selectDataVisit)
    val indexer2 = new StringIndexer().setInputCol("queries_name").setOutputCol("queries_name_index").fit(indexer1data)
    val indexer2data = indexer2.transform(indexer1data)
    //dataiphost.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //计算用户相似性
    val simData = calculateIpSim(indexer2data, spark, properties)
    //计算用户网络（关系网）
    val simNetData = calculateSimNet(simData, spark)
    //    //计算每个子网中的bot主机数量
    val botnetData = calculateBotNum(simNetData, spark, properties)
    //过滤IP为0000的记录
    val result = botnetData.filter("srcip != '0.0.0.0'")
    result
  }

  //计算用户相似性矩阵
  def calculateIpSim(data: DataFrame, spark: SparkSession, properties: Properties) = {
    //spark内置的矩阵运算，提供了计算cosine相似性的API，一下步骤为矩阵格式转换操作
    val dataMatrix = data.rdd.map(
      row =>
        new MatrixEntry(row.getAs[Double]("srcip_index").toInt.toLong, row.getAs[Double]("queries_name_index").toInt.toLong, row.getAs[Double]("visit"))
    ).distinct()
    val dataCoordinateMat: CoordinateMatrix = new CoordinateMatrix(dataMatrix)
    val dataRowMatrix: RowMatrix = dataCoordinateMat.toRowMatrix()
    //计算相似度矩阵
    val simMatrix = dataRowMatrix.columnSimilarities()
    val dataSim = simMatrix.entries.map { case MatrixEntry(i, j, k) => (i, j, k) } //long、long、double
    logger.error("计算U-U相似矩阵>>>")

    //转换数据格式
    import spark.implicits._
    val dataUserSim = dataSim.map(row => (row._1, row._2, row._3.formatted("%.3f"))).toDF("user1", "user2", "sim") //long、long、string
    //    dataUserSim.show(100, false)

    //关联获取原始IP和相似度
    data.createOrReplaceTempView("modeldata")
    dataUserSim.createOrReplaceTempView("userSim")
    val sim = properties.getProperty("botnet.sim.threshold").toDouble
    val userSimResult = spark.sql(
      s"""select distinct
        |       cast(a.srcip as string) srcip1,
        |       cast(a.srcip_index as int) srcip1_index,
        |       cast(a.probability as String) probability1,
        |       cast(a.sim as string),
        |       cast(b.srcip as string) srcip2,
        |       cast(b.srcip_index as int) srcip2_index,
        |       cast(b.probability as String) probability2
        |from (select cast(t.user1 as int),
        |             cast(t.user2 as int),
        |             cast(t.sim as string),
        |             cast(d.srcip as string),
        |             cast(d.srcip_index as int),
        |             cast(d.probability as string)
        |      from userSim t left join  modeldata d
        |      on cast(t.user1 as int) = cast(d.srcip_index as int)) a
        |left join modeldata b
        |on cast(a.user2 as int) = cast(b.srcip_index as int)
        |where cast(a.sim as double) >= $sim
      """.stripMargin)
    userSimResult
  }


  def calculateSimNet(data: DataFrame, spark: SparkSession) = {
       import spark.implicits._
    //社交网络
    //获取点
    val srcip1 = data.select("srcip1_index", "srcip1", "probability1")
    val srcip2 = data.select("srcip2_index", "srcip2", "probability2")
    val point: RDD[(VertexId, (String, String))] = srcip1.union(srcip2).rdd.map {
      line =>
        ((line.getAs[Int]("srcip1_index") + 1).toLong, (line.getAs[String]("srcip1"), line.getAs[String]("probability1")))
    }
    //获取线
    val connect = data.select("srcip1_index", "srcip2_index", "sim").rdd
      .map {
      line =>
        var srcip1_index = line.getAs[Int]("srcip1_index") + 1
        var srcip2_index = line.getAs[Int]("srcip2_index") + 1
        var sim = line.getAs[String]("sim")
        //判断是否为空值
        if (srcip1_index == srcip2_index) {
          srcip2_index = 0
        }
        Edge(srcip1_index.toLong, srcip2_index.toLong, sim)
    }
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("no", "Missing")
    // Build the initial Graph
    val graph = Graph(point, connect, defaultUser)
    val net = graph.connectedComponents().vertices
    val botnetData = point.join(net).distinct().map {
      line =>
        var srcip = line._2._1._1
        var probability = line._2._1._2
        val subnet = line._2._2
        if (srcip == null) {
          srcip = "0.0.0.0"
        }
        if (probability == null) {
          probability = "0"
        }
        (srcip, probability.toDouble, subnet.toString)
    }.toDF("srcip", "probability", "subnet").distinct()
    logger.error("计算子网络>>>")
    botnetData.filter("srcip != '0.0.0.0'")
  }

  //计算子网中的僵尸主机数
  def calculateBotNum(data: DataFrame, spark: SparkSession, properties: Properties) = {
    val botnetProbability = properties.getProperty("botnet.probability").toDouble
    data.createOrReplaceTempView("botdata")
    val result = spark.sql(
      s"""select t.srcip, cast(t.probability as string), cast(t.subnet as string), cast(s.botnet_num as string), cast(s.subnet_pc_num as string), cast(s.subnet_botnet_probability as string)
         |from botdata t
         |left join (select subnet, sum(botnet_num)/count(distinct srcip) as subnet_botnet_probability, sum(botnet_num) as botnet_num, count(distinct srcip) as subnet_pc_num
         |          from (select distinct srcip, subnet,
         |                       case when probability >= $botnetProbability then 1 else 0 end as botnet_num
         |                from botdata)
         |          group by subnet)  s
         |on t.subnet = s.subnet
      """.stripMargin)
    //新增id列
    val filterNum = org.apache.spark.sql.functions.udf(() => {
      UUID.randomUUID().toString
    })
    val resultId = result.withColumn("id", filterNum())
    logger.error("计算子网络中的僵尸主机>>>")
    resultId
  }

  //关联历史数据
  def connetctHistory(botnetDF: DataFrame, originDF: DataFrame, spark: SparkSession, properties: Properties) = {
    //将原始记录连接在成一个string
    import spark.implicits._
    val colname = originDF.columns.mkString("#")
    val originDFConnect = originDF.rdd.map {
      line =>
        //        val allMessage = line.toSeq.map(_.toString).mkString("#")
        val allMessage = line.getAs[String]("srcip")+"#"+
          line.getAs[String]("dstip")+"#"+
          line.getAs[String]("srcport")+"#"+
          line.getAs[String]("dstport")+"#"+
          line.getAs[String]("queries_name")
        val ip = line.getAs[String]("srcip")
        (ip, colname, allMessage)
    }.toDF("srcip", "colname", "origin_data")
    //将相同srcip的记录，使用'###'进行连接合并
    originDFConnect.createOrReplaceTempView("originDFConnect")
    val originDFConnectAgg = spark.sql(
      """select srcip, colname, CONCAT_WS('###', COLLECT_SET(origin_data)) AS origin_data
        |from originDFConnect
        |group by srcip, colname
      """.stripMargin)
    //对每一个ip，筛选出10条原始记录
    val filterNum = org.apache.spark.sql.functions.udf((str: String) => {
      val splitResult = str.split("""###""")
      val result = if (splitResult.length <= 10) splitResult.mkString("###") else splitResult.take(10).mkString("###")
      result
    })
    val originDFConnectAggLimit = originDFConnectAgg.withColumn("trace", filterNum(originDFConnectAgg("origin_data")))
    //原始数据的其他信息关联
    val originDFOtherMess = originDF.select("srcip", "scountry", "sprovince", "scity", "slatitude", "slongitude").distinct().toDF("srcip", "srccountry", "srcprovince", "srccity", "srclatitude", "srclongtitude")
    //对结果df关联原始df
    botnetDF.createOrReplaceTempView("botnetDF")
    originDFConnectAggLimit.createOrReplaceTempView("originDFConnectAggLimit")
    originDFOtherMess.createOrReplaceTempView("originDFSelect")
    val result = spark.sql(
      """select a.id, a.srcip, a.probability, a.subnet, a.botnet_num, a.subnet_pc_num, a.subnet_botnet_probability, a.colname, a.trace, b.srccountry, b.srcprovince, b.srccity, b.srclatitude, b.srclongtitude
        |from (select s.id, s.srcip, s.probability, s.subnet, s.botnet_num, s.subnet_pc_num, s.subnet_botnet_probability, t.colname, t.trace
        |      from botnetDF s
        |      left join originDFConnectAggLimit t
        |      on s.srcip = t.srcip) a
        |left join originDFSelect b
        |on a.srcip = b.srcip
      """.stripMargin)
    logger.error("关联原始记录>>>")
    //    result.show(5,false)
    result.persist(StorageLevel.MEMORY_AND_DISK_SER)
    result
  }


  def connHistory(botnetDF: DataFrame, originDF: DataFrame, spark: SparkSession, properties: Properties) = {

    //找出每个僵尸网络中访问最多的前10个域名
    //子网络标记
    val subnet = botnetDF.select("subnet").distinct().rdd.map(_.getAs[String]("subnet")).collect()
    val subnetData = subnet.map{
      net =>
        val ip = botnetDF.filter(s"subnet = '$net'").rdd.map(_.getAs[String]("srcip")).collect()
        val subnetData = originDF.select("srcip", "queries_name", "answers_address").filter(line => ip.contains(line.getAs[String]("srcip"))).distinct()
        subnetData.createOrReplaceTempView("subnetData")
        val domainCount = spark.sql(
          s"""
             |select ${net} as subnet, queries_name, count(queries_name) as count
             |from subnetData
             |group by queries_name
             |order by count(queries_name)
             |limit 10
           """.stripMargin)
        domainCount.createOrReplaceTempView("domainCount")
        domainCount
    }
    //连接子网结果
    val subnetUnion = subnetData.reduce((x,y) => x.union(y))
    subnetUnion.createOrReplaceTempView("subnetResult")
    val subnetResult = spark.sql(
      """
        |select subnet ,CONCAT_WS('#', COLLECT_SET(queries_name)) AS trace
        |from subnetResult
        |group by subnet
      """.stripMargin)
    //关联子网结果和原始信息到僵尸网络结果
    val botnetDFJoinSubnetResult = botnetDF.join(subnetResult, Seq("subnet"), "left_outer")
    val originDFOtherMess = originDF.select("srcip", "scountry", "sprovince", "scity", "slatitude", "slongitude").distinct().toDF("srcip", "srccountry", "srcprovince", "srccity", "srclatitude", "srclongtitude")
    val result = botnetDFJoinSubnetResult.join(originDFOtherMess, Seq("srcip"), "left_outer")
    logger.error("关联原始记录>>>")
    result.show(5, false)
    result
  }


}
