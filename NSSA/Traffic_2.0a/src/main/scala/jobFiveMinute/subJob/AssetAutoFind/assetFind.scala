package jobFiveMinute.subJob.AssetAutoFind

import java.io.{BufferedInputStream, File, FileInputStream, FileWriter}
import java.net.URI
import java.util.{Properties, UUID}
import java.util.{Calendar, Date}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, _}
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession, _}
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import scala.util.Try
import jobFiveMinute.subJob.AssetAutoFind.subClass._

/**
  * Created by Administrator on 2018/6/12.
  */
class assetFind(data: DataFrame, spark: SparkSession, properties: Properties) extends LoggerSupport with Serializable with saveToKAFKA{

  def main() = {
    //配置日志
    Logger.getLogger("org").setLevel(Level.ERROR) //显示的日志级别
    var directory = new File("..")
    PropertyConfigurator.configure(directory + "/conf/assetsFindlog4j.properties")

    //判断是否存在空数据
    if (data != null) {
      try {
        logger_assetsfind.error("读取资产库表")
        val findIP = getNeIp(spark, properties)
        logger_assetsfind.error("筛选出新发现资产")
        //过滤出满足条件的ip
        val newDFs = getNewAsset(spark, data, findIP)
        //取唯一的记录
        val findResult = filterAsset(newDFs, spark, properties)

        //新资产入库
        if (newDFs._1.count() >= 1 || newDFs._2.count() >= 1) {
          ////            saveNewAsset(findResult, properties)
          saveToPostgre(findResult, properties) //存到postgreSQL
//          toKafka(spark, (properties.getProperty("kafka.nodes"),properties.getProperty("kafka.topic.assetsfind")), findResult.toJSON)
          logger_assetsfind.error("入库成功！")
        }
      }
      catch {
        case e: Exception => logger_assetsfind.error(" asset find false!")
      }
    }
  }


  //spark连接
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

  //写文件
  def writeFile(newFile:List[String]) = {
    val targetFile = properties.getProperty("file.past")
    val file = new File(targetFile).exists()
    if(!file){
      new File(targetFile).createNewFile()
    }
    val fileW = new FileWriter(properties.getProperty("file.past"), true)
    for (file <- newFile)
      fileW.write("\n" + file)
    fileW.close()
    logger_assetsfind.error("写入配置文件成功")
  }

  //获取原始数据
  def readData() = {
    //获取hdfs文件
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(properties.getProperty("hdfs.netflow.path")), conf)
    val fileList: Array[FileStatus] = hdfs.listStatus(new Path(properties.getProperty("hdfs.netflow.path")))
    //获取hdfs目录导数第二个文件
    val file = fileList.map(_.getPath.toString).sorted
    val targetFile = file(file.length-2)
    //    logger_assetsfind.error("hdfs文件:"+file.toList)
    logger_assetsfind.error("原始hdfs文件:"+targetFile)
    hdfs.close()
    //读取所有文件数据
    val oriData = spark.read.json(targetFile)
    logger_assetsfind.error("原始数据")
    oriData.show(3)
    oriData
  }


  //获得资产库以及已经被发现的IP
  def getNeIp(spark: SparkSession,properties: Properties) = {
    import spark.implicits._
    val dTable = properties.getProperty("asset.table")
    val fTable = properties.getProperty("find.table")
    val deviceTable = s"(SELECT DEVICE_IP AS ip FROM $dTable WHERE STATUS !=2 ) AS devip"
    val findTable = s"(SELECT ip FROM $fTable WHERE FLG !=1 ) AS findip"//
    //   资产库IP
    val jdbcDF_DEV = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", properties.getProperty("postgre.address"))
      .option("dbtable", deviceTable)
      .option("user",properties.getProperty("postgre.user"))
      .option("password",properties.getProperty("postgre.password"))
      .load()
    //已经被发现IP
    val jdbcDF_FIND = spark.read.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", properties.getProperty("postgre.address"))
      .option("dbtable", findTable)
      .option("user",properties.getProperty("postgre.user"))
      .option("password",properties.getProperty("postgre.password"))
      .load()
    val result = jdbcDF_DEV.union(jdbcDF_FIND).map(_.getAs[String]("ip")).collect()
    logger_assetsfind.error("资产库和新发现资产表的ip总数："+result.length)
    logger_assetsfind.error("资产库的ip总数："+jdbcDF_DEV.map(_.getAs[String]("ip")).collect().length)
    logger_assetsfind.error("新发现资产表的ip总数："+jdbcDF_FIND.map(_.getAs[String]("ip")).collect().length)

    result
  }

  //筛选新发现资产
  def getNewAsset(spark: SparkSession,netflowDF: DataFrame,neips: Array[String])={

    //过滤出源IP在网段内且未被发现的 而且不是ICMP的
    val ipt = properties.getProperty("target.ip")
    val srcDF = netflowDF.filter{
      flow=>
        val srcip = flow.getAs[String]("srcip")
        val ipList = srcip.split("\\.").toList
        val ipSeg = List(ipList.head,ipList(1),ipList(2)).mkString(".")
        val ipSets: List[String] = ipt.split(",").toList
        val srcFlag = ipSets.contains(ipSeg) && !neips.contains(srcip) && flow.getAs[String]("protocol")!="icmp"
        srcFlag
    }
    //过滤出目的IP在网段内且未被发现 而且 是TCP而且包数量大于2 而且不是ICMP的
    val dstDF = netflowDF.filter{
      flow=>
      val dstip = flow.getAs[String]("dstip")
      val f2 = !neips.contains(dstip)
      val f3 = flow.getAs[String]("protocol")=="tcp"&& Try(flow.getAs[String]("uppkts").toDouble>2).getOrElse(false)
      val ipList = dstip.split("\\.").toList
      val ipSeg = List(ipList.head,ipList(1),ipList(2)).mkString(".")
      val ipSets: List[String] = ipt.split(",").toList
      val f1 = ipSets.contains(ipSeg)
      f1 && f2 && f3
    }
    (srcDF,dstDF)
  }

  def inIPSets(ip:String, ipt:String) = {
    val ipList = ip.split("\\.").toList
    val ipSeg = List(ipList.head,ipList(1),ipList(2)).mkString(".")
    val ipSets: List[String] = ipt.split(",").toList
    ipSets.contains(ipSeg)
  }


  def filterAsset(ipDF:(Dataset[Row], Dataset[Row]), spark: SparkSession,properties: Properties) = {
    import spark.implicits._
    //带上ID
    val idudf=org.apache.spark.sql.functions.udf(()=>{UUID.randomUUID().toString})
    //转换时间
    val addTime=org.apache.spark.sql.functions.udf((time:String)=>{
      Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time.toLong * 1000))
    })
    //添加flg
    val idFlg=org.apache.spark.sql.functions.udf(()=>{0})
    //添加isnew
    val idIsnew=org.apache.spark.sql.functions.udf(()=>{0})
    //转换字段名 并且找出发现ip的最新时间的记录
    //根据源ip发现
    val srcip = ipDF._1.select("srcip").rdd.map{case Row(a) => a.toString}.distinct().collect()
    val srcOri = ipDF._1.select("srcip", "dstip", "protocol", "srcport", "recordtime", "uppkts")
    val srcipDF = srcip.map{ line => srcOri.filter(s"srcip = '$line'").take(1).head}.map{case Row(a,b,c,d,e,f) => (a.toString, a.toString,b.toString,c.toString,d.toString,e.toString,f.toString)}
    val srcDF = spark.sparkContext.parallelize(srcipDF).toDF("ip", "srcip", "dstip", "protocol", "port", "recordtime", "packagenum")//
    val srcRe = srcDF.withColumn("recordid",idudf()).withColumn("flg",idFlg()).withColumn("isnew",idIsnew()).withColumn("storagetime",addTime(srcDF("recordtime"))).select("recordid","ip", "srcip", "dstip", "protocol", "port", "storagetime", "packagenum","flg","isnew")

    //根据目的IP发现
    val dstip = ipDF._2.select("dstip").rdd.map{case Row(a) => a.toString}.distinct().collect()
    val dstOri = ipDF._2.select("srcip", "dstip", "protocol", "srcport", "recordtime", "downpkts")
    val dstipDF = dstip.map{ line => dstOri.filter(s"dstip = '$line'").take(1).head}.map{case Row(a,b,c,d,e,f) => (a.toString, a.toString,b.toString,c.toString,d.toString,e.toString,f.toString)}
    val dstDF = spark.sparkContext.parallelize(dstipDF).toDF("ip", "srcip", "dstip", "protocol", "port", "recordtime", "packagenum").withColumn("recordid",idudf())
    val dstRe = dstDF.withColumn("recordid",idudf()).withColumn("flg",idFlg()).withColumn("isnew",idIsnew()).withColumn("storagetime",addTime(dstDF("recordtime"))).select("recordid","ip", "srcip", "dstip", "protocol", "port", "storagetime", "packagenum","flg","isnew")

    logger_assetsfind.error("根据源IP发现的资产")
    srcRe.show(10,false)
    logger_assetsfind.error("根据目的IP发现的资产")
    dstRe.show(10,false)
    srcRe.union(dstRe)
  }


  //转换字段名，入库
  def saveNewAsset(data: DataFrame ,properties: Properties)={
    //入库
    val postgprop = new Properties()
    postgprop.put("user",properties.getProperty("postgre.user"))
    postgprop.put("password",properties.getProperty("postgre.password"))
    data.write.mode(SaveMode.Append).jdbc(properties.getProperty("postgre.address"),"T_SIEM_DEV_FIND", postgprop)
  }

  def saveToPostgre(data: DataFrame ,properties: Properties) = {
    val table = properties.getProperty("find.table")
    logger_assetsfind.error("开始导入POSTGRESQL>>>")
    data.foreachPartition {
      part =>
        val conn_str = properties.getProperty("postgre.address")
        //        val conn_str = "jdbc:postgresql://172.16.1.108:5432/NXSOC5"
        Class.forName("org.postgresql.Driver").newInstance
        val conn = DriverManager.getConnection(conn_str, properties.getProperty("postgre.user"), properties.getProperty("postgre.password"))
        part.foreach {
          line =>
            try {
              val sqlText =
                s"""INSERT INTO $table
                   |(recordid, ip, srcip,
                   |  dstip, protocol, port,
                   |  storagetime, packagenum, flg,
                   |  isnew)
                   |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              prep.setString(1, line.getAs[String]("recordid"))
              prep.setString(2, line.getAs[String]("ip"))
              prep.setString(3, line.getAs[String]("srcip"))
              prep.setString(4, line.getAs[String]("dstip"))
              prep.setString(5, line.getAs[String]("protocol"))
              prep.setString(6, line.getAs[String]("port"))
              prep.setTimestamp(7, line.getAs[Timestamp]("storagetime"))
              prep.setString(8, line.getAs[String]("packagenum"))
              prep.setInt(9, line.getAs[Int]("flg"))
              prep.setInt(10, line.getAs[Int]("isnew"))
              prep.executeUpdate
            } catch {
              case e: Exception => logger_assetsfind.error("导入出错" + e.getMessage)
            }
            finally {}
        }
        conn.close

    }

  }












}
