package jobOneHour.subJob.dnsCovDetect

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import jobOneHour.subJob.dnsCovDetect.FeatureSubstract.{addNegLabel, addPosLabel}

import scala.tools.cmd.Property

class RFModelTrain(properties: Properties) {

  def train(){
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().
    setMaster(properties.getProperty("dnsTask.master")).
    setAppName(properties.getProperty("dnsTaskTrain.Name")).
    set("es.nodes", properties.getProperty("es.nodes")).
    set("es.port", properties.getProperty("es.port"))

  //    新建spark对象
  val spark = SparkSession.builder().config(conf).getOrCreate()
  spark.conf.set("spark.sql.crossJoin.enabled", true)
  val sc = spark.sparkContext
//    训练数据格式是dns流量+label
  val InputFile = properties.getProperty("dnsTaskTrain.InputFileNormal")
  val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> InputFile)
  val normalData = spark.read.format("com.databricks.spark.csv").options(options).load().limit(5000)
    .persist()
  normalData.show()

  val InputFile4 = properties.getProperty("dnsTaskTrain.InputFileAbnormal")
  val options4 = Map("header" -> "true", "delimiter" -> "\t", "path" -> InputFile4)
  val outlierData = spark.read.format("com.databricks.spark.csv").options(options4).load()
    .persist()

  val normalData_label = addNegLabel(normalData)
  val outlierData_label = addPosLabel(outlierData)
  val mergeData_label = normalData_label.union(outlierData_label)
  val processData = new FeatureSubstract().addFea(mergeData_label,"train")

  //      对数据进行预处理，进行编码转换
  val EncodingData = new FeatureEncoder(processData).feaEncode()
  val AssembleData = new FeatureEncoder(EncodingData).assData()


  //    划分训练集，测试集
  val trainSize = 0.7
  val testSize = 0.3


  val Array(trainingData, testData) = AssembleData.randomSplit(Array(trainSize, testSize),21)


  println("assembler")


//  testData.createOrReplaceTempView("testData_view")
//  trainingData.select("proto", "flags_response", "flags_truncated", "size",
//    "question_rrs", "answer_rrs", "authority_rrs", "additional_rrs_index", "queries_type_index",
//    "maxAnswersTTL", "maxAnswersTXT").show()

  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int](0 -> 5, 5 -> 7, 3 -> 5) //有多个离散特征怎么办？
  val numTrees = 4 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "gini"
  val maxDepth = 4
  val maxBins = 10

  val RF = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(numTrees)
    .setMaxBins(maxBins)
    //      .setImpurity(impurity)
    .setMaxDepth(maxDepth)
    .setFeatureSubsetStrategy(featureSubsetStrategy)

  val paramGrid = new ParamGridBuilder()
    //      .addGrid(RF.numTrees,Array(5,10,15,20))
    //      .addGrid(RF.maxDepth,Array(5,10,20))
    //      .addGrid(RF.impurity,Array("gini","entropy"))
    //      .addGrid(RF.featureSubsetStrategy,Array("auto","sqrt","0.2","0.5"))
    .build()

  val nFolds: Int = 5

  //    val evaluator = new MulticlassClassificationEvaluator()
  //      .setLabelCol("label")
  //      .setPredictionCol("prediction")
  //      .setMetricName("weightedFalsePositiveRate")

  val evaluator = new MyMulticlassEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("f1")

  val cv = new CrossValidator()
    .setEstimator(RF)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(nFolds)

  //    使用trainingData训练模型
  val CrossModel: CrossValidatorModel = cv.fit(trainingData)

  val model = CrossModel
    .bestModel.asInstanceOf[RandomForestClassificationModel]

  def trainModel()={
    println("bestmodel")
    println(model.toDebugString)
    println("model saved")
    model.write.overwrite().save(properties.getProperty("hdfs.path") + properties.getProperty("dnsTaskTrain.model"))
    model
  }

  }
}
