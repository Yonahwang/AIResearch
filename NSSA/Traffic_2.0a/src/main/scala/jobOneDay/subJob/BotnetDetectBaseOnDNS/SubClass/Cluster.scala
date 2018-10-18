package jobOneDay.subJob.BotnetDetectBaseOnDNS.SubClass

import breeze.linalg.{DenseVector, _}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession, _}


/**
  * Created by wzd on 2018/1/30.
  */
class Cluster extends loggerSet{

  def main(data:DataFrame, spark:SparkSession)={
    logger.error("cluster >>> begin")
    //聚类
    val (clusterResult, featureList) = fit(data,spark)
    //Map(类别, 特征名称, 聚类得分, 特征次数)
    val labelDomainMap = calculateCooccurScores(clusterResult,featureList, spark)
    //Map(ip, class)
    val ipLabeMap = ipLabelHash(clusterResult)
    logger.error("cluster >>> end")
    (labelDomainMap, ipLabeMap)
  }




  def fit(data:DataFrame, spark:SparkSession) = {

    import spark.implicits._
    var result: DataFrame = null
    var domainName:List[String] = List()
    try {

      //将域名进行hash映射,映射成double索引
      val indexerProcess = new StringIndexer().setInputCol("queries_name").setOutputCol("queries_name_index").fit(data)
      //one hot 处理
      val onehostProcess = new OneHotEncoder().setInputCol(indexerProcess.getOutputCol).setOutputCol("domain_name_onehot_vector")
      //转换成向量形式
      val vectorProcess = new VectorAssembler().setInputCols(Array(onehostProcess.getOutputCol)).setOutputCol("featureVec")
      //使用pipeline管道进行模型拟合
      val pipelineStage = Array(indexerProcess, onehostProcess, vectorProcess)
      val pipline = new Pipeline().setStages(pipelineStage)
      val pipModel = pipline.fit(data)
      val pipModel1DF = pipModel.transform(data)

      //获取index对应的域名
      domainName = indexerProcess.labels.toList
      logger.error("cluster >>> one hot")
//      //由于没有找到整合数据的api，故需要用非管道方法来进行整合
//      //根据IP划分，统计每种域名访问的次数
//      pipModel1DF.createOrReplaceTempView("pipModel1DF")
//      val groupByDF = spark.sql(
//        """select srcip, queries_name, domain_name_onehot_vector_2, count(queries_name) as DNCount
//          |from pipModel1DF
//          |group by srcip, queries_name, domain_name_onehot_vector_2
//        """.stripMargin)
//      //用访问次数乘以Vector
//      import spark.implicits._
//      val addVec = org.apache.spark.sql.functions.udf((vec: Vector, DNCount: Long) => {
//        Vectors.dense(vec.toArray.map(_ * DNCount.toDouble))
//      })
//      val weightDF = groupByDF.withColumn("featureVec", addVec(groupByDF("domain_name_onehot_vector_2"), groupByDF("DNCount")))
//      weightDF.show(5)
      val srcip = data.rdd.map(_.getAs[String]("srcip")).distinct().collect()
      val aggregationResult = srcip.map{
        eachip =>
          val tempVector= pipModel1DF.filter(s"srcip = '$eachip'").rdd.map(_.getAs[Vector]("featureVec"))
          val sumVector = tempVector.reduce{
            (x, y) =>{
              val temp = DenseVector(x.toArray) + DenseVector(y.toArray)
              Vectors.dense(temp.toArray)
            }
          }
          (eachip, sumVector)
      }
      val aggregationResultDF = spark.sparkContext.parallelize(aggregationResult).toDF("srcip", "featureVec")
      logger.error("cluster >>> reduce vector")
      //继续通过管道函数进行处理
      //归一化处理
      val scaleProcess = new MinMaxScaler().setInputCol("featureVec").setOutputCol("SclFeatures")
      //kmeans
      val kmeansProcess = new KMeans().setK(3).setFeaturesCol("featureVec").setPredictionCol("class")
      //使用pipeline管道进行模型拟合
      val pipelineStage2 = Array(scaleProcess, kmeansProcess)
      val pipline2 = new Pipeline().setStages(pipelineStage2)
      val pipModel2 = pipline2.fit(aggregationResultDF)
      val modelResult2 = pipModel2.transform(aggregationResultDF)//.select("srcip", "Prediction", "Probability")
      logger.error("cluster >>> kmeans model")
//      modelResult2.show(10, false)
      //过滤聚类结果数小于3个IP的记录
      val saveLabel = (0 to 3).toList.filter{
        line =>
          modelResult2.filter(s"class = $line").count().toInt >= 3
      }
      logger.error("cluster >>> label leave:" + saveLabel)
      result = modelResult2.filter{
        line =>
          saveLabel.contains(line.getAs[Int]("class"))
      }
    }
    catch {
      case e: Exception => {
        if (data.count() < 2) {
          logger.error("the length of input dataframe < 2, can not run Kmeans model.")
        }
        logger.error("Kmeans Process false!!!")
      }
    } finally {}
    (result, domainName)
  }


  def calculateCooccurScores(data:DataFrame,featureList:List[String], spark:SparkSession) = {
    val label = data.select("class").distinct().collect().map{case Row(a) => a.toString}
    val result: Map[String, Map[String, (Double, Double)]] = label.map{
      each =>
        val tempData = data.filter(s"class = $each")
        //创建DenseMatrix对象
        //DenseMatrix对象是按列来填充数据的
        //生成的时候，转换一下，行为特征，列为样本
        val dataList = tempData.rdd.flatMap(_.getAs[Vector]("featureVec").toArray).collect()
        val colNum: Int = tempData.count().toInt
        val rowNum: Int = featureList.length -1
        var mat = new DenseMatrix[Double](rows = rowNum, cols = colNum, data = dataList)
        var mat2 = mat.copy
        logger.error("cluster >>> mat")
        //使用DenseMatrix对象的row sum方法计算每个域名被多少IP访问
        //大于1的元素替换成1
        for(i <- 0 to rowNum-1){
          for(j <- 0 to colNum-1){
            if(mat2(i,j)>0.0){
              mat2(i,j) = 1.0
            }
          }
        }
        //计算共现次数最多的域名
        val colSum = sum(mat2, Axis._1)
        val colSumMat = sum(mat, Axis._1)
//        logger.error("cluster >>> mat >>> label:"+each)
//        logger.error("cluster >>> mat >>> colSum:"+colSum)

        //选出域名访问次数大于10的域名index
        logger.error("cluster >>> domain score")
        val index_seq = 0 to featureList.length-1
        val sum_index = index_seq.zip(colSum.toArray)
        val sum_index_sample = sum_index.sortWith(_._2>_._2).filter(_._2 != 0.0).take(10).toList
        //计算Q(类别-域名)，维度为n(ip) * min(domain, 10)
        val ip_count = tempData.select("srcip").distinct().count().toDouble
        val eachResult = sum_index_sample.map{
          line =>
            (featureList(line._1), (line._2/ip_count, colSumMat(line._1)))//特征名称,聚类得分,特征次数
        }.toMap
//        logger.error("cluster >>> domain score >>> featureList:"+featureList)
        logger.error("cluster >>> domain score >>> eachResult:"+eachResult)
        (each, eachResult)
    }.toMap
    result
  }

  def ipLabelHash(data:DataFrame)={
    val labelList = data.select("srcip","class").distinct().collect().map{
      case Row(a,b) => (a.toString,b.toString)
    }.toMap
    labelList
  }




}
