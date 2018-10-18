package jobOneHour.subJob.dnsCovDetect

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import jobOneHour.subClass.LoggerSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql._
import jobOneHour.subJob.dnsCovDetect.readHDFS._
import org.apache.log4j.Logger


/**
  * 使用预训练的模型，对DNS流量进行预测。
  *
  * Created by TangLe on 2018/3/13
  */

class RFModelDetect extends LoggerSupport with Serializable {

  /**
    * 检测进行隐蔽通信的DNS异常流量
    */
  def detect(properties: Properties, spark: SparkSession, dnsData: DataFrame){

    logger.info("--------------start DNS COVERT CHANNEL DETECTION-----------------")
    logger.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))



//    try{

//    对数据进行预处理，提取特征
      val processData = new FeatureSubstract().addFea(dnsData, "predict")

//    对数据进行预处理，进行编码转换
      val EncodingData = new FeatureEncoder(processData).feaEncode()
      val AssembleData = new FeatureEncoder(EncodingData).assData()

//    加载预处理模型
//      val modelPath = config.getString("dnsTaskDet.model")
      val modelPath = properties.getProperty("hdfs.path") + properties.getProperty("dnsTaskDet.model")
      println(modelPath)
//      val modelPathLocal = config.getString("dnsTaskDet.model.local")
      val modelExist = dirExists(modelPath)
      println(modelExist)

      if(modelExist) {
        val RFModel_test = RandomForestClassificationModel.load(modelPath)
        //        println(RFModel_test.uid + "  " + RFModel_test.numFeatures + "  "+ RFModel_test.getNumTrees)
        //        RFModel_test.write.overwrite().save(modelPath)
        val predictions = RFModel_test.transform(AssembleData).persist()


        //    异常数据展示
        import spark.implicits._
        val abn = predictions.filter($"prediction" === 1.0)
        println("This is result")
        println(abn.count())
        abn.show(2, false)

        //    异常数据入库
        new SaveResult(predictions, spark, properties).save()
        println("save succeed")

        //      将本次已经处理的文件名存入visitTag
//        saveVisited(spark, filteredFileNames)
      }
//      }
//  catch{
//    case e: Exception =>
//      println(e)
//  }

//    spark.stop()
  }

  def dirExists(hdfsDirectory: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(new URI(hdfsDirectory),conf,"root")
//    println("jieguo = " + fs.exists(new Path(hdfsDirectory)))
    val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
    return exists
  }



}



