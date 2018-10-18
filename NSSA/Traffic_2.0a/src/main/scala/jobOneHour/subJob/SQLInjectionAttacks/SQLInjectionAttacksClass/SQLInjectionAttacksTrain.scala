package jobOneHour.subJob.SQLInjectionAttacks.SQLInjectionAttacksClass

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import jobOneHour.subClass.LoggerSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.log4j.Logger
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by TTyb on 2017/12/12.
  */

case class URID(label: Double, urltext: String) extends Serializable {}

class SQLInjectionAttacksTrain(properties: Properties, spark: SparkSession,httpData:DataFrame) extends LoggerSupport with Serializable with ConfusionMatrix {
  //保存训练集文件
  def saveTrainFile(): Unit = {
    val negativeDataPath = properties.getProperty("NegativeDataPath")
    val positiveDataPath = properties.getProperty("PositiveDataPath")
    //获得负样本
    val negativeData = divideurl(getData(negativeDataPath), 0.00)
    negativeData.show(10, false)
    //获得正样本
    val positiveData = divideurl(getField(getData(positiveDataPath).limit(negativeData.count().toInt)), 1.00)
    positiveData.show(10, false)
    //合并切割
    val trainData = negativeData.union(positiveData).toDF()
    val splits = trainData.randomSplit(Array(0.7, 0.3))
    val (trainingDF, testingDF) = (splits(0), splits(1))

    //计算TF-IDF值
    val tokenizer = new Tokenizer().setInputCol("urltext").setOutputCol("urlwords")
    val hashingTF = new HashingTF().setInputCol("urlwords").setOutputCol("rawFeatures").setNumFeatures(20000)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    //声明贝叶斯模型
    val bayesModel = new NaiveBayes()
      .setFeaturesCol("features")
      .setPredictionCol("predictionNB")
      .setProbabilityCol("probabilityNB")
      .setRawPredictionCol("rawpredictionNB")

    //流水线作业
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, bayesModel))
    val model: PipelineModel = pipeline.fit(trainingDF)
    savemodel(model)

    //获得混淆矩阵
    val testpredictionAndLabel = model.transform(testingDF)
    val confusionMatrixDataFrame = getConfusionMatrix(testpredictionAndLabel, spark)
    confusionMatrixDataFrame.show(false)
  }


  //正则提取问号后面的内容
  def getField(dataFrame: DataFrame): DataFrame = {
    val numpattern = """(.+?)(\?)(.*?)""".r
    import spark.implicits._
    val newData = dataFrame.rdd.map {
      row =>
        val uri = row.getAs[String]("uri")
        val newuri = uri match {
          case numpattern(a, b, c) => c
          case _ => "none"
        }
        newuri
    }.toDF("uri").filter(_ (0) != "none")
    newData
  }

  //切分url
  def divideurl(moniflowdata: DataFrame, label: Double) = {
    val arr = List("%20", "%21", "%22", "%23", "%24", "%25", "%26", "%27", "%28", "%29", "%2A", "%2B", "%2C", "%2F", "%3A", "%3B", "%3C", "%3D", "%3E", "%3F", "%40", "%5C", "%7C")
    val arr1 = List(" ", "!", "\"", "#", "$", "%", "&", "'", "(", ")", "*", "+", ",", "/", ":", ";", "<", "=", ">", "?", "@", "\\", "|")
    val array = arr.zip(arr1)
    import spark.implicits._
    val divideddata = moniflowdata.mapPartitions {
      partIt =>
        val partres = partIt.map {
          line =>
            val preuri = line.getAs[String]("uri")
            var tmp = URID(0.00, "")
            if (preuri != null) {
              try {
                val urid = label
                //var uri: String = ""
                var uri = preuri.map {
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
                uri = preuri.mkString("")
                for (item <- array) {
                  uri = uri.replace(item._2, item._1 + " ")
                }
                if (uri.head == ' ') {
                  uri = uri.drop(1)
                }
                tmp = URID(urid, uri)
              }
              catch {
                case e: Exception => {
                  logger.error(s"url解析失败!错误" + "\t" + e.getMessage + "\t" + "错误url为" + preuri)
                }
              }
            }
            tmp
        }
        partres
    }
    divideddata.toDF("label", "urltext")
  }

  //读取数据
  def getData(dataPath: String): DataFrame = {
    var dataAll: DataFrame = null
    if (dataPath.contains("negetive")) {
      logger.error("获取训练数据")
      val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> dataPath)
      dataAll = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
    } else if (dataPath.contains("positive")) {
      val arrayFile = getFileName()
      //读取dataframe
      val dataFrameList: List[DataFrame] = arrayFile.map {
        path =>
          logger.error(path)
          var result: DataFrame = null
          try {
            val dataOptions = Map("header" -> "true", "delimiter" -> "\t", "path" -> path)
            result = spark.read.options(dataOptions).format("com.databricks.spark.csv").load()
          } catch {
            case e: Exception => logger.error("出错：" + e.getMessage)
          }
          result
      }.toList.filter(_ != null)

      //合并dataframe
      val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
      dataAll = dataFrameList.tail.foldRight(dataFrameList.head)(unionFun)
    }
    dataAll
  }

  //保存训练模型结果
  def savemodel(Model: PipelineModel): Unit = {
    val modelPath = properties.getProperty("TrainModelPath")
    //保存模型
    Model.write.overwrite.save(modelPath)
    logger.error("保存成功")
  }

  //获取文件的名字
  def getFileName(): Array[String] = {
    val filePath: String = properties.getProperty("hdfsUrl") + properties.getProperty("httpHDFS")
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd0000")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    val nowTime = new Date()
    val nowDay = dateFormat.format(nowTime)
    logger.error(yesterday + "-" + nowDay)

    //获取前缀后缀
    var prefix = ""
    var suffix = ""

    var arrayFile: Array[String] = new ArrayBuffer[String]().toArray

    val configuration = new Configuration()
    val output = new Path(filePath)
    val hdfs = output.getFileSystem(configuration)
    val fs = hdfs.listStatus(output)
    val fileName = FileUtil.stat2Paths(fs)
    hdfs.close()

    fileName.foreach { eachfile =>
      val eachFileName = eachfile.getName.split("\\.")
      prefix = eachFileName.head.replace(filePath, "").replace("netflow", "")
      suffix = eachFileName.last
      if (prefix.toLong >= yesterday.toLong && prefix.toLong < nowDay.toLong) {
        arrayFile = arrayFile :+ filePath + "/" + prefix.toLong.toString + "." + suffix
      }
    }
    arrayFile
  }

}
