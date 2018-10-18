package jobOneDay.subJob.Botnet

import java.io.{BufferedInputStream, File, FileInputStream, _}
import java.util.Properties

import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

import scala.util.Try


/**
  * Created by Administrator on 2018/1/12.
  */

object botNet {

  //log4j日志
  @transient lazy val logger = Logger.getLogger(this.getClass)
  Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
  var directory = new File("src/main")
  //  var logFilePath = directory.getAbsolutePath
  PropertyConfigurator.configure(directory + "/conf/botnetlog4j.properties")


  def main(args: Array[String]): Unit = {

    //读取配置文件(默认读取前一个目录的conf，如果不存在，则读取本目录的conf)
    var ipstream = Try(new BufferedInputStream(new FileInputStream(new File("src/main") + "/conf/botnet.properties")))
      .getOrElse(new BufferedInputStream(new FileInputStream(new File(".") + "/conf/botnet.properties")))
    var properties: Properties = new Properties()
    properties.load(ipstream)

    //配置spark环境
    val spark = getSparkSession(properties)
    val sc: SparkContext = spark.sparkContext
    val sql: SQLContext = spark.sqlContext

    //读取数据（ES）
    //    val oriData = getESDate(spark, properties)
    //    saveToLocal(oriData)
    //读取本地文件
    val oriData = readLocal(properties, spark)
    //获取恶意域名检测结果
    val maliciousDomain =getMaliciousDomain(spark, properties)
    //关联出有访问过恶意域名的IP
    val requestIP = getRequestIP(oriData, maliciousDomain, spark, properties)
    //    saveToFile(requestIP, properties.getProperty("bot.net.result"), "requestIP.csv")
    //计算僵尸主机概率
    val botProbability = calculateBotProbability(requestIP, spark, properties)
    //保存到本地文件
    saveToFile(botProbability, properties.getProperty("bot.pc.result"), "bot.csv")
    //寻找僵尸网络
    val botnetProbability = searchBotnet(botProbability, spark, properties)
    //关联历史数据
    val botnetProbabilityConnectHistory = connetctHistory(botnetProbability, requestIP, spark, properties)
    //保存到本地文件
    saveToFile(botnetProbabilityConnectHistory, properties.getProperty("bot.net.result"), "botnet.csv")

  }


  def getSparkSession(properties: Properties): SparkSession = {
    val sparkconf = new SparkConf()
      .setMaster(properties.getProperty("spark.master.url"))
      .setAppName(properties.getProperty("spark.app.name"))
      .set("spark.port.maxRetries", properties.getProperty("spark.port.maxRetries"))
      .set("spark.cores.max", properties.getProperty("spark.cores.max"))
      .set("spark.executor.memory", properties.getProperty("spark.executor.memory"))
      .set("es.nodes", properties.getProperty("es.nodes"))
      .set("es.port", properties.getProperty("es.port"))
    val Spark = SparkSession.builder().config(sparkconf).getOrCreate()
    Spark
  }

  def getESDate(spark: SparkSession,properties: Properties) = {
    //es查询语句
    val es_index = properties.getProperty("es.data.index")
    val query_mail =
      s"""{
         |  "query":{
         |    "filtered":{
         |      "query":{
         |        "match_all":{}
         |      },
         |      "filter":{
         |        "terms":{"appproto":["dns"]}
         |      }
         |    }
         |  }
         |}""".stripMargin
    val rowDF = spark.esDF(es_index, query_mail)
    val data = rowDF.select("srcip","dstip","srcport","dstport","appproto","col1","col2","col3","col4","col5","col6","col7","col8","col9","col10")
      .toDF("srcip","dstip","srcport","dstport","protocol","flags_response","flags_reply_code","domainname","Queries.type","Answers.name","Answers.type","Answers.ttl","Answers.datalength","Answers.address","association_id")
    //      .filter("appproto = 'dns' and col2 = '3'")
    logger.error("读取ES最近一天数据成功>>>")
    logger.error("共有数据量："+data.count())
    data.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data
  }

  def readLocal(properties: Properties, spark: SparkSession) = {
    import spark.implicits._
    val data = spark.sparkContext.textFile("./conf/test.txt").map{row=>
      val temp = row.split("\t")
      (temp(0), temp(5), temp(6), temp(7))
    }.toDF("srcip","flags_response","flags_reply_code","domainname")
    logger.error("读取本地数据成功>>>")
    logger.error("共有数据量："+data.count())
    //    data.show(5)
    data
  }

  //获取恶意域名名单
  def getMaliciousDomain(spark: SparkSession,properties: Properties) = {
    import spark.implicits._
    val path = properties.getProperty("malicious.domain")
    val oridomain = spark.sparkContext.textFile(path)
    //分割数据
    val domain = oridomain.map{
      line =>
        val temp = line.split("\t")
        (temp(0), temp(1))
    }.toDF("name", "label")
    logger.error("获取恶意域名名单>>>")
    domain
  }

  //关联访问过恶意域名的IP
  def getRequestIP(dnsdata:DataFrame, domaindata:DataFrame, spark: SparkSession,properties: Properties) = {
    //获取恶意域名
    val domain: Array[String] = domaindata.rdd.map(_.getAs[String]("name")).collect()
    //筛选出只访问恶意域名的dns记录
    val result = dnsdata.filter {
      line =>
        domain.contains(line.getAs[String]("domainname"))
    }.toDF()
    logger.error("过滤恶意域名IP>>>")
    result
  }

  //计算僵尸主机的概率
  def calculateBotProbability(data:DataFrame, spark: SparkSession,properties: Properties) = {
    data.createOrReplaceTempView("inputdata")
    //计算僵尸主机概率并替换了空值
    val result = spark.sql(
      """select distinct
        |       case when a.srcip = 'null' then '0.0.0.0' else a.srcip end as srcip,
        |       case when a.domainname = 'null' then 'www' else a.domainname end as domainname,
        |       cast(b.probability as string)
        |from inputdata a
        |left join (select srcip, sum(sum_code)/count(sum_code) as probability
        |           from (select distinct srcip, domainname,
        |                        case when flags_reply_code = '3' then 1 else 0 end as sum_code
        |                 from inputdata)
        |           group by srcip) b
        |on a.srcip = b.srcip
      """.stripMargin)
    logger.error("计算僵尸主机概率>>>")
    result
  }

  //寻找僵尸网络
  def searchBotnet(data:DataFrame, spark: SparkSession,properties: Properties) = {
    //选择需要字段
    val addcol = org.apache.spark.sql.functions.udf((str: String) => {1.0})
    val selectDataVisit = data.withColumn("visit",addcol(data("domainname")))
    //建立索引
    val indexer1 = new StringIndexer().setInputCol("srcip").setOutputCol("srcip_index").fit(selectDataVisit)//输出double类型
    val indexer1data = indexer1.transform(selectDataVisit)
    val indexer2 = new StringIndexer().setInputCol("domainname").setOutputCol("domainname_index").fit(indexer1data)
    val indexer2data = indexer2.transform(indexer1data)
    //dataiphost.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //srcip、domainname、visit、srcip_index、domainname_index、probability
    //计算用户相似性
    val simData = calculateIpSim(indexer2data, spark)
    //计算用户网络（关系网）
    val simNetData = calculateSimNet(simData, spark)
    //    //计算每个子网中的bot主机数量
    val botnetData = calculateBotNum(simNetData, spark, properties)
    botnetData
  }


  //关联历史数据
  def connetctHistory(botnetDF:DataFrame, originDF:DataFrame, spark:SparkSession, properties: Properties) = {
    //将原始记录连接在成一个string
    import spark.implicits._
    val colname = originDF.columns.mkString("#")
    val originDFConnect = originDF.rdd.map{
      line =>
        val allMessage = line.toSeq.map(_.toString).mkString("#")
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
      val result = if(splitResult.length <= 10) splitResult.mkString("###") else splitResult.take(10).mkString("###")
      result
    })
    val originDFConnectAggLimit = originDFConnectAgg.withColumn("trace",filterNum(originDFConnectAgg("origin_data")))
    //对结果df关联原始df
    botnetDF.createOrReplaceTempView("botnetDF")
    originDFConnectAggLimit.createOrReplaceTempView("originDFConnectAggLimit")
    val result = spark.sql(
      """select s.srcip, s.probability, s.subnet, s.botnet_num, s.subnet_pc_num, s.subnet_botnet_probability, t.colname, t.trace
        |from botnetDF s
        |left join originDFConnectAggLimit t
        |on s.srcip = t.srcip
      """.stripMargin)
    logger.error("关联原始记录>>>")
    //    result.show(5,false)
    result
  }



  //计算用户相似性矩阵
  def calculateIpSim(data: DataFrame, spark: SparkSession) = {
    //spark内置的矩阵运算，提供了计算cosine相似性的API，一下步骤为矩阵格式转换操作
    val dataMatrix = data.rdd.map(
      row =>
        new MatrixEntry(row.getAs[Double]("srcip_index").toInt.toLong, row.getAs[Double]("domainname_index").toInt.toLong, row.getAs[Double]("visit"))
    ).distinct()
    val dataCoordinateMat: CoordinateMatrix = new CoordinateMatrix(dataMatrix)
    val dataRowMatrix: RowMatrix = dataCoordinateMat.toRowMatrix()
    //计算相似度矩阵
    val simMatrix = dataRowMatrix.columnSimilarities()
    val dataSim = simMatrix.entries.map { case MatrixEntry(i, j, k) => (i, j, k) }//long、long、double
    logger.error("计算U-U相似矩阵>>>")

    //转换数据格式
    import spark.implicits._
    val dataUserSim = dataSim.map(row => (row._1, row._2, row._3.formatted("%.3f"))).toDF("user1", "user2", "sim")//long、long、string
    //    dataUserSim.show(100, false)

    //关联获取原始IP和相似度
    data.createOrReplaceTempView("modeldata")
    dataUserSim.createOrReplaceTempView("userSim")
    val userSimResult = spark.sql(
      """select distinct
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
      """.stripMargin)
    userSimResult
  }


  def calculateSimNet(data:DataFrame, spark: SparkSession) = {
    //数据实际输入格式
    //dataframe:srcip1(string),srcip1_index(int),srcip2(string),srcip2_index(int),probability1(string),probability2(string),sim(string)
    //数据需要输入的格式
    //dataframe:srcip1(string),srcip1_index(Long),srcip2(string),srcip2_index(Long),probability1(string),probability2(string),sim(string)
    import spark.implicits._
    //社交网络
    //获取点
    val srcip1 = data.select("srcip1_index", "srcip1", "probability1")
    val srcip2 = data.select("srcip2_index", "srcip2", "probability2")
    val point: RDD[(VertexId, (String, String))] = srcip1.union(srcip2).rdd.map{
      line =>
        ((line.getAs[Int]("srcip1_index")+1).toLong,(line.getAs[String]("srcip1"),line.getAs[String]("probability1")))
    }
    //获取线
    //    val connect = data.select(data("srcip1_index").cast("String"), data("srcip2_index").cast("String"), data("sim").cast("String")).rdd
    val connect = data.select("srcip1_index", "srcip2_index", "sim").rdd
      //      .map{
      //        case Row(srcip1_index:Double,srcip2_index:Double,sim:String) => Edge(srcip1_index.toInt.toLong, srcip2_index.toInt.toLong, sim)
      //      }
      .map{
      line =>
        var srcip1_index = line.getAs[Int]("srcip1_index") + 1
        var srcip2_index = line.getAs[Int]("srcip2_index") + 1
        var sim = line.getAs[String]("sim")
        //判断是否为空值
        //          if(srcip1_index == null){srcip1_index = 0}
        //          if(srcip2_index == null){srcip2_index = 0}
        if(srcip1_index == srcip2_index){srcip2_index = 0}
        Edge(srcip1_index.toLong, srcip2_index.toLong, sim)
      //          (srcip1_index.toLong, srcip2_index.toLong, sim)
    }
    //Define a default user in case there are relationship with missing user
    val defaultUser = ("no", "Missing")
    // Build the initial Graph
    val graph = Graph(point, connect, defaultUser)
    val net = graph.connectedComponents().vertices
    val botnetData= point.join(net).distinct().map {
      line =>
        //        println(line)
        var srcip = line._2._1._1
        var probability = line._2._1._2
        val subnet = line._2._2
        if(srcip == null) {srcip = "0.0.0.0"}
        if(probability == null) {probability = "0"}
        (srcip, probability.toDouble, subnet.toString)
    }.toDF("srcip", "probability", "subnet").distinct()
    logger.error("计算子网络>>>")
    //    botnetData.show(10, false)
    botnetData
  }

  //计算子网中的僵尸主机数
  def calculateBotNum(data:DataFrame, spark: SparkSession, properties: Properties) = {
    val botnetProbability = properties.getProperty("botnet.probability")
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
    logger.error("计算子网络中的僵尸主机>>>")
    //    result.show(10, false)
    result
  }

  //保存到文件
  def saveToFile(data:DataFrame, path:String, fileName:String) = {
    //判断待写入文件是否存在，存在则删除
    val file = new File(path, fileName).exists()
    if(file){
      new File(path, fileName).delete()
    }
    //写文件
    val column = data.columns//获取df列名
    val writer = new PrintWriter(new File(path, fileName))//创建文件读写对象
    val firstLine = column.mkString("\t") + "\n"//写入列名
    writer.write(firstLine)
    data.rdd.collect().foreach{//写入数据
      line =>
        val lineString = column.map{each=>line.getAs[String](each)}.reduce{(x, y) => x +"\t"+ y} + "\n"//将数据写到一个String
        writer.write(lineString)
    }
    writer.close()
    logger.error(s"write down $fileName!")
  }

  //  //保存僵尸主机数据到本地
  //  def saveToLocal(data:DataFrame, properties: Properties) = {
  //    val botResultPath = properties.getProperty("bot.pc.result")
  ////    val writer = new PrintWriter(new File("./conf/bot.txt" ))
  //    val writer = new PrintWriter(new File(botResultPath))
  //    writer.write("srcip\tdomainname\tprobability\n")
  //    data.rdd.collect().foreach{
  //      line =>
  //        var tempstr = ""
  //        tempstr += line.getAs[String]("srcip") + "\t"
  //        tempstr += line.getAs[String]("domainname") + "\t"
  //        tempstr += line.getAs[String]("probability") + "\n"
  //        writer.write(tempstr)
  //    }
  //    writer.close()
  //    print("write down bot!")
  //  }
  //
  //  //保存僵尸网络到本地
  //  def botnetSaveToLocal(data:DataFrame, properties: Properties) = {
  //    val botnetResultPath = properties.getProperty("bot.net.result")
  ////    val writer = new PrintWriter(new File("./conf/botnet.txt" ))
  //    val writer = new PrintWriter(new File(botnetResultPath))
  //    //srcip\tprobability\tsubnet\tbotnet_num\tsubnet_pc_num\tsubnet_botnet_pro\n
  //    writer.write("srcip\tprobability\tsubnet\tbotnet_num\tsubnet_pc_num\tsubnet_botnet_pro\n")
  //    data.rdd.collect().foreach{
  //      line =>
  //        var tempstr = ""
  //        tempstr += line.getAs[String]("srcip") + "\t"
  //        tempstr += line.getAs[String]("probability") + "\t"
  //        tempstr += line.getAs[String]("subnet") + "\t"
  //        tempstr += line.getAs[String]("botnet_num") + "\t"
  //        tempstr += line.getAs[String]("subnet_pc_num") + "\t"
  //        tempstr += line.getAs[String]("subnet_botnet_probability") + "\n"
  //        writer.write(tempstr)
  //    }
  //    writer.close()
  //    print("write down botnet!")
  //  }



}
