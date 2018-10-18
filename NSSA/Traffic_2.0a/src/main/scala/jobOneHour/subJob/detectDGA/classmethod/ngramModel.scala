package jobOneHour.subJob.detectDGA.classmethod

import scala.collection.mutable

/**
  * Created by Yiutto on 2017年12月4日 0004.
  */

trait ngramModel {
  //  1.得到所有的ungram的向量组
  def uniGrams(words: String) = {
    for (i <- 0 until words.size) yield {
      words(i).toString
    }
  }

  //  2.得到所有bigram的向量组
  def biGrams(words: String) = {
    for (i <- 0 until words.size - 1) yield {
      words(i).toString + words(i + 1).toString
    }
  }

  //  3.得到所有trigram的向量组
  def triGrams(words: String) = {
    for (j <- 0 until words.size - 2) yield {
      words(j).toString + words(j + 1).toString + words(j + 2).toString
    }
  }

  //  4.得到所有unigram的统计个数
  def unigramCounts(words: String): mutable.Map[String, Int] = {
    var unigram_count: mutable.Map[String, Int] = new mutable.HashMap[String, Int]()
    for (i <- uniGrams(words).toArray) {
      if (!unigram_count.contains(i))
        unigram_count(i) = 0
      unigram_count(i) = unigram_count(i) + 1
    }
    unigram_count
  }
}
