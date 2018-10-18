package jobOneHour.subJob.dnsCovDetect

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame


/**
  * 将所有string类型的特征转为double类型的特征
  * 包括离散特征和连续特征
  * @param Data
  */
class FeatureEncoder(Data: DataFrame) {

  val DoubleCol = Array("sumAnswerLen","numRatio","stringEntropy","queriesLabelCal","queriesNameLen",
      "maxAnswersTTL", "maxAnswersTXT"
    )

  val indexer = new StringIndexer()
    .setInputCol("additional_rrs")
    .setOutputCol("additional_rrs_index")
    .setHandleInvalid("skip")

  val indexer2 = new StringIndexer()
    .setInputCol("queries_type")
    .setOutputCol("queries_type_index")
    .setHandleInvalid("skip")

//  val assData = new VectorAssembler()
//    .setInputCols(DoubleCol)
//    .setOutputCol("features")
//    .transform(Data)

  def assData() ={
    val data = new VectorAssembler()
    .setInputCols(DoubleCol)
    .setOutputCol("features")
    .transform(Data)
    data
  }
  def feaEncode()={
    val indexedData = indexer.fit(Data).transform(Data)
    indexer2.fit(Data).transform(Data)
  }
}

object FeatureEncoder{

//  val DoubleCol = Array("sumAnswerLen","numRatio","stringEntropy","queriesLabelCal","queriesNameLen",
//    "maxAnswersTTL", "maxAnswersTXT"
//  )
//
//  val indexer = new StringIndexer()
//    .setInputCol("additional_rrs")
//    .setOutputCol("additional_rrs_index")
//    .setHandleInvalid("keep")
//
//  val indexer2 = new StringIndexer()
//    .setInputCol("queries_type")
//    .setOutputCol("queries_type_index")
//    .setHandleInvalid("keep")
//
//  val assembler = new VectorAssembler()
//    .setInputCols(DoubleCol)
//    .setOutputCol("features")
//    .transform(data)
//
//  def feaEncode(processData: DataFrame)={
//    val indexedData = indexer.fit(processData).transform(processData)
//    indexer2.fit(indexedData).transform(indexedData)
//  }

}