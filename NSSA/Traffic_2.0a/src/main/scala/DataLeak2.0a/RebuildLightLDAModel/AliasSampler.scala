package SituationAwareness_2_0a.RebuildLightLDAModel

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 构造alias采样表。提供了两个方案：dense和sparse
  * 在这里，由于实数精度所导致的问题有很多，不过也许都一一修复了
  */
class AliasSampler extends Serializable {

  private var alias = ArrayBuffer[(Int, Int, Double)]()

  def build(posibilities: Array[Double]): AliasSampler = {
    build_sparse(posibilities, posibilities.zipWithIndex.map(_._2))
    this
  }

  def build_sparse(posibilities: Array[Double], indexs: Array[Int]): AliasSampler = {
    //    println("posibilities: " + posibilities.map(_.toString).reduce(_ + " " + _))
    //    println(posibilities.length)

    val arch_posibilities = posibilities.map {
      line =>
        val ad = new ArchDouble
        ad := line
        ad
    }
    val sum = arch_posibilities.reduce {
      (ele1, ele2) =>
        val res = new ArchDouble
        res := ele1 + ele2
        res
    }
    val per_max = new ArchDouble
    per_max := sum / arch_posibilities.length
    var arr_big = ArrayBuffer[(Int, ArchDouble)]()
    var arr_small = ArrayBuffer[(Int, ArchDouble)]()
    for ((ele, ind) <- arch_posibilities.zip(indexs))
      if (ele() < per_max())
        arr_small += ((ind, ele))
      else arr_big += ((ind, ele))
    var alias = ArrayBuffer[(Int, Int, Double)]()
    //    println(per_max)
    for (i <- 0 until posibilities.length) {
      if (arr_big.nonEmpty && arr_small.nonEmpty) {
        //        println(alias.length.toString + "\t" + arr_small.map(_.toString()).reduce(_ + "," + _) + "\t\t\t" +
        //          arr_big.map(_.toString()).reduce(_ + "," + _))
        val big = arr_big.head
        arr_big -= big
        val small = arr_small.head
        arr_small -= small
        val big_d = big._2
        val small_d = small._2
        val remain = new ArchDouble
        remain := big_d + small_d
        remain -= per_max
        alias += ((small._1, big._1, small_d / per_max))
        if (remain() > per_max())
          arr_big += ((big._1, remain))
        else arr_small += ((big._1, remain))
      }
      else if (arr_small.nonEmpty) {
        val bigger = arr_small.sortWith {
          (ele1, ele2) =>
            val d1 = ele1._2
            val d2 = ele2._2
            d1() > d2()
        }.head
        arr_small -= bigger
        alias += ((bigger._1, bigger._1, bigger._2 / per_max))
      }
      else if (arr_big.nonEmpty) {
        val smaller = arr_big.sortWith {
          (ele1, ele2) =>
            val d1 = ele1._2
            val d2 = ele2._2
            d1() < d2()
        }.head
        arr_big -= smaller
        alias += ((smaller._1, smaller._1, smaller._2 / per_max))
      }
    }
    //    while ((arr_small.nonEmpty || arr_big.nonEmpty) && alias.length < posibilities.length) {
    //      println(alias.length.toString + "\t" + arr_small.map(_.toString()).reduce(_ + "," + _) + "\t\t\t" +
    //        arr_big.map(_.toString()).reduce(_ + "," + _))
    //      if (arr_small.nonEmpty)
    //        if (alias.length == posibilities.length - 1 && arr_big.isEmpty) {
    //          val small = arr_small.head
    //          arr_small -= small
    //          alias += ((small._1, small._1, small._2))
    //        }
    //        else {
    //          val small = arr_small.head
    //          arr_small -= small
    //          val big = arr_big.head
    //          arr_big -= big
    //          alias += ((small._1, big._1, small._2 / per_max))
    //          val remain = big._2 + small._2 - per_max
    //          if (remain > 0)
    //            if (remain < per_max)
    //              arr_small += ((big._1, remain))
    //            else
    //              arr_big += ((big._1, remain))
    //        }
    //      else {
    //        val big = arr_big.head
    //        arr_big -= big
    //        alias += ((big._1, big._1, 1))
    //        val remain = big._2 - per_max
    //        if (remain > 0)
    //          if (remain < per_max)
    //            arr_small += ((big._1, remain))
    //          else
    //            arr_big += ((big._1, remain))
    //      }
    //    }
    //    println((for (i <- 0 until 50) yield "-").reduce(_ + _))
    //    println(posibilities.map(_.toString).reduce(_ + " " + _))
    //    println(alias.map(_.toString).reduce(_ + " " + _))
    this.alias = alias
    this
  }

  def sample(): Int = {
    val ran = new Random()
    val k = ran.nextInt(alias.length)
    val res = if (ran.nextDouble() > alias(k)._3) alias(k)._2 else alias(k)._1
    res
  }

  /**
    * 一个也许会友好的方法。
    * 主要是，当只需要在其中若干类里随机取值时
    */
  def limit_sample(limit: Array[Int]): Int = {
    var limit_arr = limit.map(ele => (ele, 0.0)).toMap
    for (ele <- alias) {
      //小于阈值，是前者，否则是后者
      if (limit_arr.contains(ele._1))
        limit_arr += (ele._1 -> (ele._3 + limit_arr(ele._1)))
      if (limit_arr.contains(ele._2))
        limit_arr += (ele._2 -> (1 - ele._3 + limit_arr(ele._2)))
    }
    val posi_arr = limit_arr.filter(_._2 > 0).toArray
    var sum = posi_arr.map(_._2).sum
    var pointor = sum * new Random().nextDouble()
    var res = -1
    var k = 0
    //    limit_arr.foreach(println)
    //    println(sum, pointor)
    while (pointor > 0) {
      val ele = posi_arr(k)
      pointor -= ele._2
      if (pointor <= 0)
        res = ele._1
      k += 1
    }
    res
  }

  def sample_test(): Unit = {
    val arr = for (i <- 0 until 10000) yield sample()
    val count_arr = arr.groupBy(ele => ele).map(line => (line._1, line._2.size))
      .toArray.sortWith((ele1, ele2) => ele1._1 < ele2._1)
    val show_str = count_arr.map(_._2.toString).reduce(_ + " " + _)
    println(show_str)

    (for (i <- 0 until 10000) yield this.limit_sample(Array(0, 1, 4))).groupBy(ele => ele)
      .map(line => (line._1, line._2.length)).foreach(println)
  }
}
