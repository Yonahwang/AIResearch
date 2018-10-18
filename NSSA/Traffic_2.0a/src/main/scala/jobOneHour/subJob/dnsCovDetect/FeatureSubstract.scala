package jobOneHour.subJob.dnsCovDetect

import breeze.linalg.{max, sum}
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

class FeatureSubstract extends Serializable {

  def fillNA(mergeData: DataFrame) ={
//    mergeData.printSchema()
    val data = mergeData.na.fill("0").na.fill(Map("answers_data_length"->"0","answers_ttl" -> "0","answers_type" -> "0","answers_txt_length" -> "0"))
    println("fillNull")
    data
  }

  def addFea(data: DataFrame, index: String)={
    val mergeData  = fillNA(data)
//    mergeData.printSchema()
//    mergeData.show()
    var mergeData2 = data
    if (index == "train"){
      mergeData2 = mergeData.withColumn("maxAnswersTTL", calMaxTTL_udf(mergeData("answers_ttl")))
        .withColumn("maxAnswersTXT", calMaxTxt_udf(mergeData("answers_txt_length")))
        .withColumn("queriesNameLen", domainLen_udf(mergeData("queries_name")))
        .withColumn("queriesLabelCal", calSubDomain_udf(mergeData("queries_name")))
        .withColumn("stringEntropy", calEntro_udf(mergeData("queries_name")))
        .withColumn("numRatio", calNumRat_udf(mergeData("queries_name")))
        .withColumn("sumAnswerLen", calAnswerLen_udf(mergeData("answers_data_length")))
        .select("flow_id","timestamp","srcip","dstip","srcport","dstport", "queries_name","queriesNameLen", "queriesLabelCal", "stringEntropy", "numRatio", "sumAnswerLen", "protocol","proto", "flags_response", "flags_truncated", "size",
          "question_rrs", "answer_rrs", "authority_rrs", "additional_rrs", "queries_type", "maxAnswersTTL", "maxAnswersTXT","slatitude","slongitude","sprovince","scountry","scity","dlatitude","dlongitude","dprovince","dcountry","dcity","label")
        .withColumn("size1",mergeData("size").cast("Double"))
        .drop("size")
        .withColumnRenamed("size1","size")
    }
    else{
      mergeData2 = mergeData.withColumn("maxAnswersTTL", calMaxTTL_udf(mergeData("answers_ttl")))
        .withColumn("maxAnswersTXT", calMaxTxt_udf(mergeData("answers_txt_length")))
        .withColumn("queriesNameLen", domainLen_udf(mergeData("queries_name")))
        .withColumn("queriesLabelCal", calSubDomain_udf(mergeData("queries_name")))
        .withColumn("stringEntropy", calEntro_udf(mergeData("queries_name")))
        .withColumn("numRatio", calNumRat_udf(mergeData("queries_name")))
        .withColumn("sumAnswerLen", calAnswerLen_udf(mergeData("answers_data_length")))
        .select("flow_id","timestamp","srcip","dstip","srcport","dstport", "queries_name","queriesNameLen", "queriesLabelCal", "stringEntropy", "numRatio", "sumAnswerLen", "protocol","proto", "flags_response", "flags_truncated", "size",
          "question_rrs", "answer_rrs", "authority_rrs", "additional_rrs", "queries_type", "maxAnswersTTL", "maxAnswersTXT","slatitude","slongitude","sprovince","scountry","scity","dlatitude","dlongitude","dprovince","dcountry","dcity")
        .withColumn("size1",mergeData("size").cast("Double"))
        .drop("size")
        .withColumnRenamed("size1","size")
    }

//    val inputCols = Array("maxAnswersTTL","maxAnswersTXT")
//    mergeData2.show()
//    val s: Array[Column] = inputCols.map(col)
//    mergeData2.select(s:_*).schema.foreach(println)
    mergeData2
  }

//  计算最大TTL值
  def calMaxTTL(ttl: String): Double = {
    if (ttl != null) {
      val maxTTL = max(ttl.split("#").toList.map(_.toDouble))
      maxTTL
    }
    else
      0.0
  }

  //    请求区域的域名字段，多级域名个数
  def calSubDomain(domain: String): Int = {
    if (domain != null) {
      val subCount = domain.split("\\.").length
      subCount
    }
    else
      0
  }

  def domainLen(domain: String): Int = {
//    try{
      val domains = domain.replace(".", "")
      val len = domains.length
      len
//    }
//    catch{
//      case e: Exception => 0
//    }
  }

  //    参数：字符串；返回：各元素出现次数的列表。
  //    对字符串的元素进行计数，返回各元素出现次数的Map映射表。"aabb"=>Map("a"->2,"b"->2)
  //    用于后续计算字符串的熵。
  def calChar(domain: String): (mutable.Map[Char, Int]) = {
    val domains = domain.replace(".", "")
    val len = domains.length
    var countsMap = mutable.Map[Char, Int]()
    var i = 0
    for (i <- 0 to len - 1) {
      val ch: Char = domains.charAt(i)
      if (countsMap.contains(ch)) {
        val counts = countsMap.apply(ch)
        countsMap += (ch -> (counts + 1))
      }
      else {
        countsMap += (ch -> 1)
      }
    }
    countsMap
  }

  //    参数：字符串的字符出现次数数组；返回：熵值
  //    调用mllib.tree.impurity.Entropy的caculate方法求字符串的熵
  //    熵: sum(p*lg(p))
  def calEntro(domain: String): Double = {
//    try{
      val domains = domain.replace(".", "")
      calChar(domains)
      match {
        case countsMap => {
          Entropy.calculate(countsMap.values.toArray.map(_.toDouble), domains.length)
      }
    }
//    }
//    catch{
//      case e: Exception => -10.0
//    }
  }

  //    遍历Map映射表的逐个字符，判断是否为数字类型,返回数字类型所占长度比
  def calNumRat(domain: String): Double = {
    val domains = domain.replace(".", "")
    calChar(domains)
    match {
      case (countsMap) => {
        var digitCount = 0
        countsMap.keys.map {
          line =>
            if (line.isDigit)
              digitCount += countsMap.apply(line)
        }
        digitCount * 1.0 / domains.length
      }
    }
  }

  def calAnswerLen(lengths: String): Double = {
    try{
      val lengthArray = lengths.split("#").toList.toArray.map(_.toDouble)
      val sumLen = sum(lengthArray)
      sumLen
    }

    catch{
      case e: Exception => {
        println(lengths)
        0.0
      }
    }

  }




  //    增加一列，各区域最大TTL值
  val calMaxTTL_udf = udf(
    (ttl: String) =>
      calMaxTTL(ttl)
  )

  //    增加一列，各区域最大txt长度值
  val calMaxTxt_udf = udf(
    (ttl: String) =>
      calMaxTTL(ttl)
  )

  //    增加一列，域名级数
  val calSubDomain_udf = udf(
    (domain: String) =>
      calSubDomain(domain)
  )

  //    增加一列，域名长度
  val domainLen_udf = udf(
    (domain: String) =>
      domainLen(domain)
  )

  //    增加一列，域名字符串熵
  val calEntro_udf = udf(
    (domain: String) =>
      calEntro(domain)
  )

  //    增加一列，域名中数字比例
  val calNumRat_udf = udf(
    (domain: String) =>
      calNumRat(domain)
  )

  //    增加一列，answer字段长度
  val calAnswerLen_udf = udf(
    (answers: String) =>
      calAnswerLen(answers)
  )



  val conver_toDouble = udf(
    (s: String) => {
      if(s.length < 2)
        s.toDouble
      else
        0.0
    }
  )

}

object FeatureSubstract{
  //    对异常样本加标签
  val addLabel_udf_pos = udf(
    () =>
      1.0
  )

  //    对正常样本加标签
  val addLabel_udf_neg = udf(
    () =>
      0.0
  )

  def addPosLabel(normalData: DataFrame)={
    normalData.withColumn("label", addLabel_udf_pos())
  }

  def addNegLabel(outlierData: DataFrame)={
    outlierData.withColumn("label", addLabel_udf_neg())
  }



}
