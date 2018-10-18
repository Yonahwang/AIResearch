package jobOneDay.subJob.ScoreWeightCalculate

import java.io.{BufferedInputStream, File, FileInputStream, Serializable}
import java.sql.{DriverManager, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import jobOneDay.subJob.ScoreWeightCalculate.subClass.loggerSet
import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

import scala.util.Try
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD


/**
  * Created by Administrator on 2018/6/15.
  */
class scoreComputeMain(spark:SparkSession, properties: Properties) extends loggerSet with Serializable{


  def main() = {

    //log4j日志
    Logger.getLogger("org").setLevel(Level.ERROR)
    //显示的日志级别
//    var directory = new File("src/main")
//    PropertyConfigurator.configure(directory + "/conf/scoreWeight.properties")

    //配置spark环境
    val sql: SQLContext = spark.sqlContext

    //获取原始数据
    //    val oriDataSet = readLocal(properties, spark)
    val oriDataSet = readTrainingSet(spark, properties)
    //计算正确标签
    val trainSetOld = calculateClass(oriDataSet)
    val trainSet = trainSetOld.repartition(trainSetOld.rdd.partitions.size)
    //计算默认权重模式，使用逻辑回归计算每个子事件的权重值
    val weight_default = classifierPattern(trainSet, spark)
    //计算特殊权重模式，使用频繁项集，找出经常导致主机沦陷的子事件集合
    val special_rule = specialRule(trainSet, spark)
    //整合默认模式和特殊模式的数据
    val unionR = unionResult(weight_default, special_rule, spark)
    val result = unionR.repartition(unionR.rdd.partitions.size)
    //入库
    saveToPostgreSQLLine(result, properties)

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


  def readLocal(properties: Properties, spark: SparkSession) = {
    import spark.implicits._
    val options = Map("header" -> "true", "delimiter" -> ",", "path" -> "./data/iris.csv")
    val data = spark.read.options(options).format("com.databricks.spark.csv").load()
    val result = data.rdd.map {
      line =>
        (line.getAs[String]("fea1").toString.toDouble,
          line.getAs[String]("fea2").toString.toDouble,
          line.getAs[String]("fea3").toString.toDouble,
          line.getAs[String]("fea4").toString.toDouble,
          line.getAs[String]("label").toString.toDouble,
          line.getAs[String]("flag").toString.toDouble)
    }.toDF("fea1", "fea2", "fea3", "fea4","label","flag")
    logger.error("读取本地数据成功>>>")
    logger.error("共有数据量：" + result.count())
    result.show(5)
    result
  }

  //读取postgre上的样本数据
  def readTrainingSet(spark: SparkSession,properties: Properties) = {
    val table = properties.getProperty("postgre.attack.sample.table")
    val deviceTable = s"(SELECT * FROM $table) AS sample"
    //样本集
    import org.postgresql.Driver
    val readSet = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", properties.getProperty("postgre.address"))
      .option("dbtable", deviceTable)
      .option("user",properties.getProperty("postgre.user"))
      .option("password",properties.getProperty("postgre.password"))
      .load()
    val sampleSet = readSet.select(
      readSet("label").cast("Double"),
      readSet("flag").cast("Double"),
      readSet("event_threat").cast("Double"),
      readSet("event_security_log").cast("Double"),
      readSet("event_detect_dga").cast("Double"),
      readSet("event_traffic_anomaly").cast("Double"),
      readSet("event_heartbeat").cast("Double"),
      readSet("event_up_down").cast("Double"),
      readSet("event_covert_channel").cast("Double"),
      readSet("event_external_connection").cast("Double"),
      readSet("event_abnormal_ports").cast("Double"),
      readSet("event_botnet").cast("Double")
    )
    logger.error("样本集总数："+sampleSet.count())
    sampleSet.show(5)
    sampleSet
  }

  //计算正确标签
  def calculateClass(data:DataFrame) = {
    //    val idudf=org.apache.spark.sql.functions.udf(()=>{UUID.randomUUID().toString})
    //.withColumn("recordid",idudf())
    val addClass = org.apache.spark.sql.functions.udf((label:Double,flag:Double)
    =>{
      var result = 0.0
      if(flag == 0.0){
        result = label
      }
      if(flag == 1.0){
        result = 1.0
      }
      if(flag == 2.0){
        result = 0.0
      }
      result
    })
    val dataC = data.withColumn("class", addClass(data("label"), data("flag"))).drop("label", "flag")
    //    val result = dataC.select("class", "event_threat", "event_detect_dga", "event_traffic_anomaly", "event_heartbeat", "event_up_down", "event_covert_channel", "event_external_connection", "event_abnormal_ports", "event_botnet", "event_security_log")
    logger.error("training set >>>")
    logger.error("positive set num :" + dataC.filter("class = 1.0").count())
    logger.error("negative set num :" + dataC.filter("class = 0.0").count())
    dataC
  }





  def specialRule(data:DataFrame, spark:SparkSession) = {

    //decision tree split feature

    //apriori find
    import spark.implicits._
    //转换数据
    val FPfeature1 = List("event_threat",
      "event_security_log",
      "event_detect_dga",
      "event_traffic_anomaly",
      "event_heartbeat",
      "event_up_down",
      "event_covert_channel",
      "event_external_connection",
      "event_abnormal_ports",
      "event_botnet", "class")
    val FPfeature = data.columns
    val itemSet = data.rdd.map{
      line =>
        val getLine = line.toSeq.map(_.toString)
        val result = FPfeature1.zip(getLine).map{each => if(each._2.toDouble != 0.0) each._1 else ""}.filter(x => x != "").toArray
        result
    }
    val fpg = new FPGrowth().setNumPartitions(10).setMinSupport(0.2)//支持度（suppport ）：是交易集中同时包含A和B的交易数与所有交易数之比。Support(A=>B)=P(A∪B)=count(A∪B)/|D|
    val model = fpg.run(itemSet)
    //频繁集结果
    val minConfidence = 0.8//置信度（confidence ）： 是包含A和B交易数与包含A的交易数之比。 Confidence(A=>B)=P(B|A)=support(A∪B)/support(A)
    val confidenceR = model.generateAssociationRules(minConfidence).collect().filter{ line => (line.consequent.contains("class"))}.map{
      rule =>
        Array(rule.antecedent.mkString("#"), rule.consequent.mkString("#"), rule.confidence.toString)
    }//.toDF("antecedent", "consequent", "confidence")

    logger.error("FP process >> rule")
    confidenceR.take(5).map(x=>x.toList).foreach(println)
    confidenceR
  }


  def classifierPattern(data:DataFrame, spark:SparkSession) = {
    //构造特征向量
    val assembler = new VectorAssembler().setInputCols(Array("event_threat",
      "event_security_log",
      "event_detect_dga",
      "event_traffic_anomaly",
      "event_heartbeat",
      "event_up_down",
      "event_covert_channel",
      "event_external_connection",
      "event_abnormal_ports",
      "event_botnet"
    )).setOutputCol("features").transform(data)

    //识别因子型特征(能将大于setMaxCategories当做连续特征，小于setMaxCategories当做离散特征)
    //    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(6)
    val model = new LogisticRegression()
      .setLabelCol("class")//预测列名
      .setFeaturesCol("features")//特征列名
      .setRegParam(0.01)//正则化参数
      .setPredictionCol("prediction")
      .fit(assembler)
    //计算混淆矩阵
    val modelT = model.transform(assembler)
    val TP = modelT.filter{x => x.getAs[Double]("class") == 1.0 && x.getAs[Double]("prediction") == 1.0 }.count().toDouble
    val TN = modelT.filter{x => x.getAs[Double]("class") == 0.0 && x.getAs[Double]("prediction") == 0.0 }.count().toDouble
    val FP = modelT.filter{x => x.getAs[Double]("class") == 0.0 && x.getAs[Double]("prediction") == 1.0 }.count().toDouble
    val FN = modelT.filter{x => x.getAs[Double]("class") == 1.0 && x.getAs[Double]("prediction") == 0.0 }.count().toDouble
    println("              actual")
    println("            1\t\t0")
    println(s"predict: 1  $TP\t\t$FP")
    println(s"         0  $FN\t\t$TN")
    println("准确率 = TP/ (TP+FP)= "+TP/(TP+FP))
    println("召回率 = TP/ (TP +FN)= "+TP/(TP +FN))
    println("Accuracy = (TP+TN)/(TN+TP+FN+FP) = "+(TP+TN)/(TN+TP+FN+FP))
    println("F1-score = 2*TP/(2*TP + FP + FN) = "+2*TP/(2*TP + FP + FN))

    //计算权重
    val w = model.coefficients.toArray.toList
    println("model.coefficients: "+w)
    //当原始数据只有单一标签的情况下，使用默认的权重值
    var weight = List(0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1)
    val num = w.filter(x=> x>0.0).length
    if(num > 1){
      //normalization
      val minimum = w.filter(x=> x>0.0).min/4
      val wFilter = w.map{x => if(x < 0.0) minimum else x}
      println("filter coefficients: "+wFilter)
      weight = wFilter.map{ x => x/wFilter.sum}
      println("weight coefficients: "+weight)
    }
    weight.toArray
  }


  def unionResult(weight_default:Array[Double],special_rule:Array[Array[String]],spark:SparkSession) = {
    import spark.implicits._
    //补充其他字段：默认模式
    val add_value = Array("0", "")//weight_type, scores
    val weight_default_add = Array(weight_default).map{
      line =>
        (line(0).toString,line(1).toString,line(2).toString,line(3).toString,line(4).toString,line(5).toString,line(6).toString,line(7).toString,line(8).toString,line(9).toString,"0", "")
    }
    val weight_default_add_df = spark.sparkContext.parallelize(weight_default_add).toDF("event_threat", "event_security_log", "event_detect_dga", "event_traffic_anomaly", "event_heartbeat", "event_up_down", "event_covert_channel", "event_external_connection", "event_abnormal_ports", "event_botnet", "weight_type", "scores")
    logger.error("union >>> default")
    weight_default_add_df.show(5)

    //补充其他字段：特殊模式
    val special_rule_add = special_rule.map{
      line =>
        val antecedent = line(0).split("#")
        var event_threat = if(antecedent.contains("event_threat")) "1" else "0"
        var event_security_log = if(antecedent.contains("event_security_log")) "1" else "0"
        var event_detect_dga = if(antecedent.contains("event_detect_dga")) "1" else "0"
        var event_traffic_anomaly = if(antecedent.contains("event_traffic_anomaly")) "1" else "0"
        var event_heartbeat = if(antecedent.contains("event_heartbeat")) "1" else "0"
        var event_up_down = if(antecedent.contains("event_up_down")) "1" else "0"
        var event_covert_channel = if(antecedent.contains("event_covert_channel")) "1" else "0"
        var event_external_connection = if(antecedent.contains("event_external_connection")) "1" else "0"
        var event_abnormal_ports = if(antecedent.contains("event_abnormal_ports")) "1" else "0"
        var event_botnet = if(antecedent.contains("event_botnet")) "1" else "0"
        var weight_type = "1"
        var scores = (line(2).toDouble * 100).toString
        (event_threat,event_security_log,event_detect_dga,event_traffic_anomaly,event_heartbeat,event_up_down,event_covert_channel,event_external_connection,event_abnormal_ports,event_botnet,weight_type,scores)
    }
    val special_rule_add_df = spark.sparkContext.parallelize(special_rule_add).toDF("event_threat", "event_security_log", "event_detect_dga", "event_traffic_anomaly", "event_heartbeat", "event_up_down", "event_covert_channel", "event_external_connection", "event_abnormal_ports", "event_botnet", "weight_type", "scores")
    logger.error("union >>> special")
    special_rule_add_df.show(5)

    //合并两种结果
    val union_r = weight_default_add_df.union(special_rule_add_df).toDF()

    //添加id和时间
    val addID=org.apache.spark.sql.functions.udf(()=>{UUID.randomUUID().toString})
    val time = Timestamp.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime))
    val addTime=org.apache.spark.sql.functions.udf(() =>{time})
    val result = union_r.withColumn("id",addID()).withColumn("recordtime", addTime())
    logger.error("union >>> union")
    result.show(10)
    result
  }

  //保存到postgre
  def saveToPostgreSQLLine(data: DataFrame, properties: Properties) = {
    val table = properties.getProperty("postgre.weight.table")
    logger.error("开始导入POSTGRESQL>>>")
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
                   |(id, recordtime, event_threat,
                   |  event_security_log, event_detect_dga, event_traffic_anomaly,
                   |  event_heartbeat, event_up_down, event_covert_channel,
                   |  event_external_connection, event_abnormal_ports, event_botnet,
                   |  weight_type, scores)
                   |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """.stripMargin
              val prep = conn.prepareStatement(sqlText)
              //              prep.setTimestamp(2,new Timestamp(line.getAs[String]("date").toLong))
              prep.setString(1, line.getAs[String]("id"))
              prep.setTimestamp(2, line.getAs[Timestamp]("recordtime"))
              prep.setString(3, line.getAs[String]("event_threat"))
              prep.setString(4, line.getAs[String]("event_security_log"))
              prep.setString(5, line.getAs[String]("event_detect_dga"))
              prep.setString(6, line.getAs[String]("event_traffic_anomaly"))
              prep.setString(7, line.getAs[String]("event_heartbeat"))
              prep.setString(8, line.getAs[String]("event_up_down"))
              prep.setString(9, line.getAs[String]("event_covert_channel")) //b.srccountry, b.srcprovince, b.srccity, b.srclatitude, b.srclongtitude
              prep.setString(10, line.getAs[String]("event_external_connection"))
              prep.setString(11, line.getAs[String]("event_abnormal_ports"))
              prep.setString(12, line.getAs[String]("event_botnet"))
              prep.setString(13, line.getAs[String]("weight_type"))
              prep.setString(14, line.getAs[String]("scores"))
              prep.executeUpdate
            } catch {
              case e: Exception => logger.error("导入出错" + e.getMessage)
            }
            finally {}
        }
        conn.close

    }
    logger.error("导入完成！")
  }


}
