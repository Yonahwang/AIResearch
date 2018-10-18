package jobOneHour.subJob.SQLInjectionAttacks.SQLInjectionAttacksClass

import java.io.Serializable
import java.net.{InetAddress, URLDecoder}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobOneHour.subClass.{LoggerSupport, saveToKAFKA}
import org.apache.log4j.Logger
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._



/**
  * Created by TTyb on 2017/12/11.
  */
case class RAWMONI(id: String, date: String, srcip: String, dstip: String, uri: String, urltext: String, srcport: String, dstport: String, method: String, host: String, flagIP: String, scountry: String, sprovince: String, scity: String, slatitude: String, slongitude: String, dcountry: String, dprovince: String, dcity: String, dlatitude: String, dlongitude: String) extends Serializable {}

case class RESMONI(id: String, date: String, srcip: String, dstip: String, uri: String, srcport: String, dstport: String, method: String, host: String, flagIP: String, scountry: String, sprovince: String, scity: String, slatitude: String, slongitude: String, dcountry: String, dprovince: String, dcity: String, dlatitude: String, dlongitude: String, predictionnb: String, probabilitynb: String) extends Serializable

class SQLInjectionAttacksReadyMain(properties: Properties, spark: SparkSession,httpData:DataFrame) extends LoggerSupport with Serializable with saveToKAFKA{
  def readyMain(): Unit = {
    //贝叶斯预测的函数
    predict()
  }

  //读取数据
  def getData(): DataFrame = {
    logger.error("获取原始数据")
    httpData
  }

  //预测结果
  def predict(): Unit = {
    //获取上网行为的数据
    val modelPath = properties.getProperty("TrainModelPathSQLInjectionAttacks")
    val moniData = getData()
    val model = PipelineModel.load(modelPath)
    val dividedData = divideurl(moniData)
    val predictionDF = model.transform(dividedData).filter("predictionNB < 1")
    saveResult(predictionDF, moniData)

  }

  def saveResult(predictionDF: Dataset[Row], moniData: DataFrame) = {
    import spark.implicits._
    //预测结果
    val resDS = predictionDF.select($"id", $"date", $"srcip", $"dstip", $"uri", $"srcport", $"dstport", $"method", $"host", $"flagIP", $"scountry", $"sprovince", $"scity", $"slatitude", $"slongitude", $"dcountry", $"dprovince", $"dcity", $"dlatitude", $"dlongitude", $"predictionNB", $"probabilityNB").distinct()
    if (resDS.take(10).length != 0) {
      //这里是保存到ES或者HDFS的代码
      saveToKafka(resDS.toDF, moniData)
    }
  }

  //切分url
  def divideurl(moniflowdata: DataFrame) = {
    val arr = List("%20", "%21", "%22", "%23", "%24", "%25", "%26", "%27", "%28", "%29", "%2A", "%2B", "%2C", "%2F", "%3A", "%3B", "%3C", "%3D", "%3E", "%3F", "%40", "%5C", "%7C")
    val arr1 = List(" ", "!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "/", ":", ";", "<", "=", ">", "?", "@", "\\", "|")
    val array = arr.zip(arr1)
    import spark.implicits._
    val divideddata = moniflowdata.mapPartitions {
      partIt =>
        val partres = partIt.map {
          line =>
            val preuri = line.getAs[String]("uri")
            val host = line.getAs[String]("host")
            val srcport = line.getAs[String]("srcport")
            val dstport = line.getAs[String]("dstport")
            val method = line.getAs[String]("request_method")
            val flagIP = line.getAs[String]("flagIP")

            val scountry = line.getAs[String]("scountry")
            val sprovince = line.getAs[String]("sprovince")
            val scity = line.getAs[String]("scity")
            val slatitude = line.getAs[String]("slatitude")
            val slongitude = line.getAs[String]("slongitude")
            val dcountry = line.getAs[String]("dcountry")
            val dprovince = line.getAs[String]("dprovince")
            val dcity = line.getAs[String]("dcity")
            val dlatitude = line.getAs[String]("dlatitude")
            val dlongitude = line.getAs[String]("dlongitude")
            var tmp = RAWMONI("", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")
            if (preuri != null && host != null && dstport != null && method != null) {
              try {
                val uri = if (preuri.contains("%")) URLDecoder.decode(preuri, "UTF-8") else preuri
                var urid: String = ""
                if (uri.contains("?") && uri.contains("=")) {
                  urid = uri.map {
                    case '/' => " / "
                    case '=' => " = "
                    case '?' => " ? "
                    case '.' => " . "
                    case '-' => " - "
                    case '_' => " _ "
                    case '&' => " & "
                    case ':' => " : "
                    case ''' => " ' "
                    case '(' => " ( "
                    case ')' => " ) "
                    case ',' => " , "
                    case '|' => " | "
                    case '*' => " * "
                    case '+' => " + "
                    case ';' => " ; "
                    case s: Char => s
                  }.mkString("")
                  for (item <- array) {
                    urid = urid.replace(item._1, item._1 + " ")
                  }
                  if (urid.head == ' ') {
                    urid = urid.drop(1)
                  }

                  tmp = RAWMONI(line.getAs[String]("flow_id"), line.getAs[String]("request_date"), line.getAs[String]("srcip"), line.getAs[String]("dstip"), uri, urid, srcport, dstport, method, host, flagIP, scountry, sprovince, scity, slatitude, slongitude, dcountry, dprovince, dcity, dlatitude, dlongitude)
                }
              }
              catch {
                case e: Exception => {
                  if (!e.getMessage.contains("URLDecoder")) {
                    logger.error(s"url解析失败!错误" + "\t" + e.getMessage + "\t" + "错误url为" + preuri)
                  }
                }
              }
            }
            tmp
        }
        partres
    }.filter(_.id.length > 0)
    divideddata
  }

  //保存结果到kafka
  def saveToKafka(resultData: DataFrame, moniData: DataFrame): Unit = {
    /**
      * sql注入0
      * 恶意代码注入1
      * 主机沦陷2
      * 僵尸网络3
      */
    import spark.implicits._
    val dataFrame = resultData.rdd.map {
      line =>
        val flagIP = line.getAs[String]("flagIP")
        val srcip = line.getAs[String]("srcip")
        val dstip = line.getAs[String]("dstip")
        val srcport = line.getAs[String]("srcport")
        val dstport = line.getAs[String]("dstport")
        var ip = ""
        var port = ""
        if (flagIP == "1") {
          ip = srcip
          port = srcport
        } else if (flagIP == "2") {
          ip = dstip
          port = dstport
        }
        val id = ROWUtils.genaralROW()
        val proto = "http"
        val resultType = "0"
        val time = line.getAs[String]("date")
        val containsSelect = properties.getProperty("containsSelect")
        var typeresult = ""
        if (containsSelect == "true") {
          if (line.getAs[String]("uri").contains("select") || line.getAs[String]("uri").contains("SELECT")) {
            typeresult = line.getAs[String]("uri")
          }
        } else {
          typeresult = line.getAs[String]("uri")
        }
        val judgmentresult = line.getAs[String]("host")
        val calculategist = line.getAs[String]("method")
        val probabilityNB = line.getAs[Vector]("probabilityNB")
        val predictionNB = line.getAs[Double]("predictionNB")
        val probabilitynb = probabilityNB(predictionNB.toInt)
        var calculateresult = ""
        if (probabilitynb == 1) {
          calculateresult = probabilitynb.toString
        }
        val encryptedtunnel = ""
        val externalconnection = ""
        val abnormalports = ""
        val abnormaltraffic = ""
        val ccdga = ""
        val heartbeat = ""
        val updown = ""
        val standby01 = ""
        val standby02 = ""
        val standby03 = ""
        val position = line.getAs[String]("scountry") + "#" + line.getAs[String]("sprovince") + "#" + line.getAs[String]("scity") + "#" +
          line.getAs[String]("slatitude") + "#" + line.getAs[String]("slongitude") + "#" + line.getAs[String]("dcountry") + "#" +
          line.getAs[String]("dprovince") + "#" + line.getAs[String]("dcity") + "#" + line.getAs[String]("dlatitude") + "#" + line.getAs[String]("dlongitude")
        val reportneip = InetAddress.getLocalHost().getHostAddress
        val event_rule_id = "MODEL_FALLHOST_MV1.0_001_001"
        val event_sub_type = "AttackBasic6B1"
        val attackflag: Int = 1

        modelResult(id, ip, srcip, dstip, srcport, dstport, port, proto, resultType, time, typeresult, judgmentresult, calculategist,
          calculateresult, encryptedtunnel, externalconnection, abnormalports, abnormaltraffic, ccdga, heartbeat, updown,
          standby01, standby02, standby03, position, reportneip, event_rule_id, event_sub_type, attackflag)
    }.toDS().where("calculateresult <> ''").where("typeresult <> ''").dropDuplicates("typeresult").toDF()
    if (dataFrame.take(10).length != 0) {
      val kafkaNodes = properties.getProperty("kafka.nodes")
      val kafkaPath = properties.getProperty("kafka.topic1")
      logger.error(kafkaNodes)
      logger.error(kafkaPath)
      toKafka(spark, (kafkaNodes, kafkaPath), dataFrame)
      dataFrame
    }
  }
}
case class modelResult(id: String, ip: String, srcip: String, dstip: String, srcport: String, dstport: String, port: String, proto: String,
                       resulttype: String, time: String, typeresult: String, judgmentresult: String, calculategist: String,
                       calculateresult: String, encryptedtunnel: String, externalconnection: String, abnormalports: String,
                       abnormaltraffic: String, ccdga: String, heartbeat: String, updown: String, standby01: String,
                       standby02: String, standby03: String, position: String, reportneip: String, event_rule_id: String,
                       event_sub_type: String, attackflag: Int)