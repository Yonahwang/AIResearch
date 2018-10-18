package jobOneHour.subJob.detectDGA.classmethod

import java.io.Serializable

import org.apache.spark.broadcast.Broadcast

import scala.collection.{Seq, mutable}

/**
  * Created by Yiutto on 2017年12月4日 0004.
  */
class trainFeature extends Serializable with ngramModel {
  def getFeature(host: String,
                 tldsArray: Array[String],
                 ngramRank: Map[String, Int],
                 hmmPro: mutable.HashMap[String, mutable.HashMap[String, Double]]) = {
    val (subdomain, domain, suffix) = tldextract(host, tldsArray)
    val domain1: String = "^" + domain + "$"
    val len = domain.length.toDouble
    val entropy: Double = getEntropy(domain)
    val (unigramAvgRank, bigramAvgRank, trigramAvgRank) = averageRank(domain, ngramRank)
    val (vowelpro, digitpro) = voweldigitPro(domain)
    val reletterpro: Double = repeatletterPro(domain)
    val (conti_consonantpro, conti_digitpro) = conti_Pro(domain)
    val getdomainpro: Double = getdomainPro(domain1, hmmPro)

    val feature = Array(len, entropy, unigramAvgRank, bigramAvgRank, trigramAvgRank, vowelpro, digitpro, reletterpro,
      conti_consonantpro, conti_digitpro, -getdomainpro)
    (domain, feature)
  }


  //  1.根据"tlds-alpha-by-domain.txt"，提取domain和tlds
  def tldextract(host: String, tlds: Array[String]) = {
    var subdomain: String = "No-Parse"
    var suffix = "No-Parse"
    var domain = "No-Parse"
    //    提取host的后缀名（suffix）
    val suffixArr = tlds.filter(tld => (("\\." + tld + "\\$").r).findAllIn(host + "$").hasNext)
    if (suffixArr.length != 0) {
      val suffixLenArr = suffixArr.map(_.length)
      suffix = suffixArr(suffixLenArr.indexOf(suffixLenArr.max))
      domain = ("\\." + suffix).r.replaceAllIn(host, "")
      //是否为合法的host，方便提取
      if (domain.contains(".")) {
        val domainArr = domain.split("\\.")
        domain = domainArr(domainArr.length - 1)
        subdomain = domainArr.slice(0, domainArr.length - 1).mkString(".")
      }
    }
    (subdomain, domain, suffix)
  }

  //  2.将ls[0, 0, 0, 1, 1, 0, 1, 1, 0] 转换为[(0, 3), (1, 2), (0, 1), (1, 2), (0, 1)]
  def consecutiveCount(ls: Seq[Int]) = {
    var tag: Int = ls(0)
    var count: Int = 0
    var consecutivecount = Array[(Int, Int)]()
    for (i <- ls) {
      if (i != tag) {
        consecutivecount = consecutivecount :+ (tag, count)
        count = 0
      }
      count = count + 1
      tag = i
    }
    //    将最后一次遍历完成后的结果保存
    consecutivecount = consecutivecount :+ (tag, count)
    consecutivecount
  }

  //  3.根据Alexa中ngram模型排名(topAlexaNgramRank.csv)来计算domain的unigram、bigram、trigram平均排名
  def averageRank(domain: String, ngramrank: Map[String, Int]): (Double, Double, Double) = {
    val domain1: String = "^" + domain + "$"
    val unigramRank =
      for (key <- uniGrams(domain)) yield {
        if (ngramrank.contains(key))
          ngramrank(key)
        else
          0
      }
    val bigramRank =
      for (key <- biGrams(domain1)) yield {
        if (ngramrank.contains(key))
          ngramrank(key)
        else
          0
      }
    val trigramRank =
      for (key <- triGrams(domain1)) yield {
        if (ngramrank.contains(key))
          ngramrank(key)
        else
          0
      }
    var averagerank1: Double = 0.0
    var averagerank2: Double = 0.0
    var averagerank3: Double = 0.0
    if (unigramRank.length != 0)
      averagerank1 = (unigramRank.toList.sum.toDouble) / (unigramRank.length)
    if (bigramRank.length != 0)
      averagerank2 = (bigramRank.toList.sum.toDouble) / (bigramRank.length)
    if (trigramRank.length != 0)
      averagerank3 = (trigramRank.toList.sum.toDouble) / (trigramRank.length)
    (averagerank1, averagerank2, averagerank3)
  }

  //  4.domain的信息熵
  def getEntropy(domain: String): Double = {
    val domainLen: Int = domain.length
    val wordfrequency = unigramCounts(domain)
    val words = wordfrequency.keys.toList
    val entropyList = words.map { line =>
      var wordpro: Double = wordfrequency(line).toDouble / domainLen
      var entropytemp = -(wordpro * (math.log(wordpro)))
      entropytemp
    }
    val entropy: Double = entropyList.sum
    entropy
  }

  //  5.domain中的元音字母概率、数字概率
  def voweldigitPro(domain: String): (Double, Double) = {
    val vowels = List("a", "e", "i", "o", "u")
    val digits = Set("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
    val words: Seq[String] = uniGrams(domain)
    var sum1: Double = 0.0
    var sum2: Double = 0.0
    for (i <- words) {
      if (vowels.contains(i))
        sum1 = sum1 + 1
      if (digits.contains(i))
        sum2 = sum2 + 1
    }
    (sum1 / domain.length, sum2 / domain.length)
  }

  //  6.domain的重复字母概率
  def repeatletterPro(domain: String) = {
    val unigramcounts: mutable.Map[String, Int] = unigramCounts(domain)
    val alphas = "abcdefghijklmnopqrstuvwxyz".toCharArray.map(line => line.toString)
    var sum: Double = 0.0
    for ((key, value) <- unigramcounts) {
      if (value > 1 && alphas.contains(key))
        sum = sum + value
    }
    sum / domain.length
  }

  //  7.domain中存在连续的辅音字母频率
  def conti_Pro(domain: String): (Double, Double) = {
    val consonant = Set('b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'p', 'q', 'r', 's', 't',
      'v', 'w', 'x', 'y', 'z')
    val digits = Set('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    //    将所有辅音字母字符转换为1，所有数字字符转化为2 ，其他转换为0
    val digit_map = domain.map { i =>
      var temp = 0
      if (consonant.contains(i))
        temp = 1
      if (digits.contains(i))
        temp = 2
      temp
    }
    val consecutivecount: Array[(Int, Int)] = consecutiveCount(digit_map)
    var sum1 = 0.0
    var sum2 = 0.0
    //    统计domain中（连续辅音字母个数>1）的总个数
    for ((x, y) <- consecutivecount) {
      if (x == 1 && y > 1)
        sum1 = sum1 + y
      if (x == 2 && y > 1)
        sum2 = sum2 + y
    }
    (sum1 / domain.length, sum2 / domain.length)
  }

  //  8.根据HMMPro计算生成domain的概率
  def getdomainPro(domain: String, topdomainHMMTrain: mutable.HashMap[String,
    mutable.HashMap[String, Double]]): Double = {
    val bigrams: Seq[String] = biGrams(domain)
    val bigramFirst: String = bigrams(0)
    //如果未发现其转移概率，则将propre设为1/38/38
    var pro: Double = 1 / 38 / 38
    if (topdomainHMMTrain("").contains(bigramFirst))
      pro = topdomainHMMTrain("")(bigramFirst)
    for (i <- 0 to bigrams.length - 2) {
      var bigram1 = bigrams(i)
      var bigram2 = bigrams(i + 1)
      var pronext: Double = 1 / 38 / 38
      if (topdomainHMMTrain.contains(bigram1)) {
        if (topdomainHMMTrain(bigram1).contains(bigram2))
          pronext = topdomainHMMTrain(bigram1)(bigram2)
      }
      pro = pro * pronext
    }
    //probability is too low to be non-DGA
    if (pro < math.exp(-120.0))
      pro = -999.0
    else
      pro = math.log(pro)
    pro
  }

}
