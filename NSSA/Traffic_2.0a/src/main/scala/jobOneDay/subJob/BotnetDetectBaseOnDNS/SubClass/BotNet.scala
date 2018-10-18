package jobOneDay.subJob.BotnetDetectBaseOnDNS.SubClass

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession, _}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

/**
  * Created by wzd on 2018/1/17.
  */


//ip,域名,域名访问次数,类别,key对的群组得分,key对的周期性得分,bot得分,vec周期
case class botPro(srcip:String, domain:String, domain_count:String, Label:String, ipDomainScore:Double, ak:Double, botProba:Double, vec:Vector, vecName:String)

//cluster 和 period 的ip数不一样，应该以period为标准
class BotNet extends Serializable with loggerSet{

  def main(labelDomainMap: Map[String, Map[String, (Double, Double)]], ipLabeMap: Map[String, String], periodResult: Dataset[periodR], spark:SparkSession, properties:Properties) = {
    logger.error("botnet >>> begin")
    //计算每个IP是僵尸主机的概率
    val ipBotProbability = calculateBot(labelDomainMap, ipLabeMap, periodResult, spark, properties)
    ipBotProbability.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //计算所在组别是僵尸网络的概率
    val botnetMap = calculateBotnet(ipBotProbability, ipLabeMap, properties)
    //转换为一个IP对应一条结果
    val transferResult = transferFormat(ipBotProbability, botnetMap, ipLabeMap, labelDomainMap, spark, properties)
    logger.error("botnet >>> end")
    transferResult
  }


  def calculateBot(labelDomainMap: Map[String, Map[String, (Double, Double)]], ipLabeMap: Map[String, String], periodResult: Dataset[periodR], spark:SparkSession, properties: Properties) = {
    //遍历周期结果
    import spark.implicits._
    val botPrabability = periodResult.rdd.mapPartitions{
      part =>
        val k1 = properties.getProperty("botnet.k1").toDouble
        val k2 = properties.getProperty("botnet.k2").toDouble
        part.map{
          line =>
            val ipDomainScore = labelDomainMap(ipLabeMap(line.srcip))(line.domain)//ipDomainScore._2:类访问该域名次数
            val botProba = k1 * ipDomainScore._1 + k2 * line.ak
            botPro(line.srcip, line.domain, ipDomainScore._2.toString, ipLabeMap(line.srcip), ipDomainScore._1, line.ak, botProba, line.vec, line.vecName)//ip,域名,域名访问次数,类别,key对的群组得分,key对的周期性得分,bot得分
        }
    }.toDS()
    logger.error("botnet >>> sum score")
    botPrabability
  }


  def calculateBotnet(ipBotProbability: Dataset[botPro], ipLabeMap: Map[String, String], properties: Properties) = {
    //计算每个label的得分
    val label = ipLabeMap.values.toList
    val k3 = properties.getProperty("botnet.k3").toDouble
    val labelScoreMap = label.map{
      line =>
        val temp = ipBotProbability.filter(x => x.Label == line)
        val tempFi = temp.filter(x => x.botProba >= k3)
        val score = tempFi.rdd.map(x=>x.srcip).distinct().count().toDouble///temp.rdd.map(x=>x.srcip).distinct().count().toDouble
        (line, score)
    }.toMap
    logger.error("botnet >>> botnet score")
    labelScoreMap
  }

  def transferFormat(ipBotProbability:Dataset[botPro], botnetMap: Map[String, Double], ipLabeMap: Map[String, String], labelDomainMap: Map[String, Map[String, (Double, Double)]], spark:SparkSession, properties:Properties) = {
    //输入：
    //ipBotProbability: ip,域名,域名访问次数,类别,key对的群组得分,key对的周期性得分,bot得分
    //botnetMap: label, botnet scores
    //ipLabeMap: ip, label
    //labelDomainMap: label, domain, 聚类得分, 域名访问次数
    //沦陷记过表：ip, status
    //输出：
    //ip,srcip,dstip,recordtime,state,label,domain_name,interval_session,malicious_name,malicious_name_count,class_malicious_name,class_malicious_name_count,bot_probability,botnet_probability
    //IP,记录时间,主机状态（0:失陷、1:可疑）,结果类型（0：DNS、1：NETFLOW）,类别,
    // 间歇性访问的域名,间歇性会话时间段,间歇性访问的次数,所在类别是僵尸网络的概率,常访问的恶意域名,常访问的域名次数,僵尸主机的概率
    import spark.implicits._
    //读取沦陷结果表
    val occupyIP = readFromOccupyResult(spark, properties)
    //获取Map(ip:dm1#dm2#...)
    val ipDmMap = ipBotProbability.map{line=>(line.srcip,line.domain)}.rdd
      .keyBy(k => k._1)
      .reduceByKey{(x, y) => (x._1, x._2 + "#" + y._2)}
      .map{line => (line._1, line._2._2)}.collect().toMap
    //获取Map(ip:mintime#maxtime...)
    val ipDmTMap = ipBotProbability.map{
      line=>
        val mapIV =  (0 to line.vec.toArray.length).toArray.zip(line.vec.toArray).toMap
        val vecN = line.vecName.split("#")
        var time = ""
        var i = 0
        mapIV.keySet.foreach{
          k =>
            if(mapIV(k) > 0.0){
              if(i == 0){
                time = vecN(k)
              }
              else{
                time = time + "#" + vecN(k)
              }
              i = i + 1
            }
        }
        (line.srcip, time)
    }.rdd
      .keyBy(k => k._1)
      .reduceByKey{
      (x, y) =>
        val time = List(x._2, y._2)
        val t = if(x._2 == "" && y._2 == "") "" else time.min + "#" + time.max
        (x._1, t)
    }.map{k => (k._1, k._2._2)}.collect().toMap

    //需要计算的IP
    val ip: List[String] = ipLabeMap.keySet.toList.intersect(ipBotProbability.map{ k=>(k.srcip)}.distinct().collect())
    //合并botnetMap、ipLabeMap、沦陷IP的结果
    //Map: ip, label, botnet score, occupy, 域名, 域名访问次数, 页面展示字段（间歇性访问域名）
    val ipLabelBotMap = ip.map{
      x =>
        val occ = if(occupyIP.contains(x)) "0" else "1"
        val groupDm = labelDomainMap(ipLabeMap(x)).keySet.mkString("#")
        val groupDmCount = labelDomainMap(ipLabeMap(x)).values.toMap.values.mkString("#")//.keySet.mkString("#")
        (x, (ipLabeMap(x), botnetMap(ipLabeMap(x)), occ, groupDm, groupDmCount, ipDmMap(x), ipDmTMap(x)))
    }.toMap
    //广播
    val time = new Date().getTime
    val broadMap = spark.sparkContext.broadcast(ipLabelBotMap)
    val broadTime = spark.sparkContext.broadcast(time)
    val idudf=org.apache.spark.sql.functions.udf(()=>{UUID.randomUUID().toString})
    //遍历每条记录
    val ipBotProbabilityF = ipBotProbability.filter{x => ip.contains(x.srcip)}.repartition(ipBotProbability.rdd.partitions.size)
    val result = ipBotProbabilityF.mapPartitions{
      part =>
        val ipLabelBotMapBC = broadMap.value
        val timeBC = broadTime.value
        part.map{
          line =>
            //IP,记录时间,主机状态（0:失陷、1:可疑）,结果类型（0：DNS、1：NETFLOW）,类别,
            // 间歇性访问的域名,间歇性会话时间段,间歇性访问的次数,所在类别是僵尸网络的概率,常访问的恶意域名,常访问的域名次数,僵尸主机的概率
            val id = UUID.randomUUID().toString
            val ip: String = line.srcip
            val recordtime = timeBC
            val status: String = ipLabelBotMapBC(line.srcip)._3
            val result_type: String = "0"
            val label = line.Label.toInt
            val domain_name: String = line.domain
            val session_time: String = line.vecName//line.vec//待定
            val session_count: String = line.vec.toArray.map(_.toString).mkString("#")
            val botnet_probability: String = ipLabelBotMapBC(line.srcip)._2.toString
            val content: String = ipLabelBotMapBC(line.srcip)._4//类中常访问域名
            val content_count: String = ipLabelBotMapBC(line.srcip)._5//类中常访问域名访问次数
            val content_confusion = ipLabelBotMapBC(line.srcip)._2.toString
            //field01存放页面需要的字段：间歇性访问域名
            //field02存放页面需要的字段：间歇性会话时间段
            val field01 = ipLabelBotMapBC(line.srcip)._6
            val field02 = ipLabelBotMapBC(line.srcip)._7.split("#").min + "-" + ipLabelBotMapBC(line.srcip)._7.split("#").max//最小最大时间
            val field03 = line.botProba.toString
            (id,ip,recordtime,status,result_type,label,domain_name,session_time,session_count,botnet_probability,content,content_count,content_confusion,field01,field02,field03)
        }
    }.toDF("id","ip","recordtime","status","result_type","label","domain_name","session_time","session_count","botnet_probability","content","content_count","content_confusion","field01","field02","field03")
    logger.error("botnet >>> result")
//    result.show(10,false)
    result
  }



//  def readFromOccupyResult(spark:SparkSession, properties:Properties) = {
//    import spark.implicits._
//    val result = spark.sparkContext.textFile(properties.getProperty("occupy.result")).map(line => (line.split("\t")(0),line.split("\t")(1))).toDF("ip", "status")
//    val ip = result.select("ip").rdd.map{case Row(a) => a.toString}.collect()
//    ip
//  }

  def readFromOccupyResult(spark:SparkSession, properties:Properties) = {
    val tableName = properties.getProperty("es.data.index.occupy")//获取配置文件中的es索引/类型（如cfsim/cfsim）
    //抓取时间范围
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val time = cal.getTime.getTime
    val start = new SimpleDateFormat("yyyy-MM-dd").format(time) + " 00:00:00"
    val end = new SimpleDateFormat("yyyy-MM-dd").format(time) + " 23:59:59"
        val starttime:Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(start).getTime
        val endtime:Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(end).getTime
//    val starttime :Long= 1518333604
//    val endtime :Long= 1518333606
    val query = "{\"query\":{\"range\":{\"time\":{\"gte\":" + starttime*1000 + ",\"lte\":" + endtime*1000 + "}}}}"
    //抓取数据下来
    val result1 = spark.esDF(tableName, query)
    val result2 = result1.filter{ line => line.getAs[String]("resultType") == "2" }
    val result3 = result2.select("ip").rdd.map{case Row(a)=> a.toString}.collect()
    logger.error("主机沦陷ip个数 >>> "+result3.length)
    result3
  }



}
