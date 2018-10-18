package SituationAwareness_2_0a.RebuildLightLDAModel

import java.io._

import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, Matrix => BM, Vector => BV}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector => SPV, Vectors => SPVs}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 利用mappartition的性质。
  * 数据将存放于[[RDD]]中，并且不再交换RDD中的数据。仅有model在网络中传输。
  *
  * 参考别人的源码后，确定一点：z，nkv，ndk，nk是需要shuffle起来，并进行统计的。
  * 而文档数据被分配到各个节点上后，就不再移动。
  *
  * 仔细思考口，lda不适合抓取一部分样本后，再进行训练。这样也许会非常严重的影响收敛速度——因为每次的变动率无法超过
  * 采样比例。
  * 不过，对于加入新数据集的训练，也许有效？特别是新数据集相对于旧数据集特别小的时候
  *
  * 2018-6-21改进：对于同一篇文档中的相同的词，只构建一次采样。
  * -   会因此改变的主要结构是z
  * 2018-6-27Debug:出现一个神必的问题：words table长度为0。
  */
class LightLDAModel(spark: SparkSession) extends Serializable {
  private var per_partition_size = 100000
  private var K = 10
  private var V = 0
  private var D = 0
  private var alpha = 0.01
  private var beta = 0.1
  private var iter_times = 20
  private var mh_steps = 2
  private var sub_sample = 1.0
  private var static_nv = Map[Int, Int]()
  private var old_nkv: BM[Int] = null
  private var index_word = Map[Int, String]()
  private var index_tag = Map[Int, String]()

  private var docs: RDD[(Int, Array[Int])] = null
  private var bc_K: Broadcast[Int] = null
  private var bc_V: Broadcast[Int] = null
  private var bc_D: Broadcast[Int] = null
  private var bc_alpha: Broadcast[Double] = null
  private var bc_beta: Broadcast[Double] = null
  private var bc_sum_alpha: Broadcast[Double] = null
  private var bc_V_beta: Broadcast[Double] = null
  private var bc_denominator_part_beta_nk_or_beta: Broadcast[Double] = null
  private var bc_denominator_nk_or_beta: Broadcast[Double] = null
  private var bc_word_index: Broadcast[Map[String, Int]] = null
  private var bc_index_word: Broadcast[Map[Int, String]] = null
  private var bc_special_class: Broadcast[Map[Int, Array[Int]]] = null
  private var bc_z: Broadcast[Map[Int, Array[Int]]] = null
  private var bc_nkv: Broadcast[BM[Int]] = null
  private var bc_ndk: Broadcast[BM[Int]] = null
  private var bc_nk: Broadcast[BV[Int]] = null
  private var bc_alpha_table: Broadcast[AliasSampler] = null
  private var bc_mh_steps: Broadcast[Int] = null


  def set_K(k: Int): LightLDAModel = {
    this.K = k
    this
  }

  def set_alpha(alpha: Double): LightLDAModel = {
    this.alpha = alpha
    this
  }

  def set_beta(beta: Double): LightLDAModel = {
    this.beta = beta
    this
  }

  def set_mh_steps(steps: Int): LightLDAModel = {
    this.mh_steps = steps
    this
  }

  def set_iter_time(times: Int): LightLDAModel = {
    this.iter_times = times
    this
  }

  def set_sub_sample(rate: Double): LightLDAModel = {
    this.sub_sample = rate
    this
  }

  /**
    * 把传入的文档，按词进行随机分配主题。并且统计得出z，nkv，ndk，nk
    *
    * @param documents
    */
  def init(documents: Documents): LightLDAModel = {
    docs = documents.docs
    index_word = documents.index_word
    index_tag = documents.index_tag
    bc_word_index = spark.sparkContext.broadcast(documents.word_index)
    bc_index_word = spark.sparkContext.broadcast(documents.index_word)
    bc_special_class = spark.sparkContext.broadcast(documents.special_class)

    V = bc_word_index.value.size
    D = docs.count().toInt

    //接下来，开始初始化z，nkv，ndk，nk
    val (z, nkv, ndk, nk, nv) = docs.mapPartitions {
      files =>
        val ran = new Random()
        var z = Map[Int, Array[Int]]()
        var nkv = BM.zeros[Int](K, V)
        var ndk = BM.zeros[Int](D, K)
        var nk = BV.zeros[Int](K)
        var nv = BV.zeros[Int](V)

        for (file <- files) {
          val word_seq = file._2
          //注意特殊类的构造
          val doc_topic = for (word <- word_seq) yield {
            val res = if (bc_special_class.value.contains(word))
              bc_special_class.value(word)(ran.nextInt(bc_special_class.value(word).length))
            else ran.nextInt(K)
            res
          }
          z += (file._1 -> doc_topic)
          //          println("doc & topic:" + file._2.map(_.toString).reduce(_ + " " + _))
          for ((word, topic) <- word_seq.zip(doc_topic)) {
            nkv(topic, word) = nkv(topic, word) + 1
            ndk(file._1, topic) = ndk(file._1, topic) + 1
            nk(topic) = nk(topic) + 1
            nv(word) = nv(word) + 1
          }
        }
        Iterator((z, nkv, ndk, nk, nv))
    }
      .reduce {
        (ele1, ele2) =>
          var z = ele1._1 ++ ele2._1
          var nkv = ele1._2 + ele2._2
          var ndk = ele1._3 + ele2._3
          var nk = ele1._4 + ele2._4
          var nv = ele1._5 + ele2._5
          (z, nkv, ndk, nk, nv)
      }


    static_nv = (for (i <- 0 until V) yield i).zip(nv.toArray).toMap
    //    z.take(5).foreach(ele => println(ele._1.toString + " " + ele._2.map(_.toString).reduce(_ + " " + _)))
    bc_z = spark.sparkContext.broadcast(z)
    bc_nkv = spark.sparkContext.broadcast(nkv.toDenseMatrix)
    bc_ndk = spark.sparkContext.broadcast(ndk.toDenseMatrix)
    bc_nk = spark.sparkContext.broadcast(nk)
    //构造alphatable
    val alpha_tabel = new AliasSampler().build((for (i <- 0 until K) yield 1.0).toArray)
    bc_alpha_table = spark.sparkContext.broadcast(alpha_tabel)
    bc_K = spark.sparkContext.broadcast(K)
    bc_V = spark.sparkContext.broadcast(V)
    bc_D = spark.sparkContext.broadcast(D)
    bc_alpha = spark.sparkContext.broadcast(alpha)
    bc_beta = spark.sparkContext.broadcast(beta)
    bc_sum_alpha = spark.sparkContext.broadcast(alpha * K)
    bc_V_beta = spark.sparkContext.broadcast(V * beta)
    bc_denominator_part_beta_nk_or_beta = spark.sparkContext.broadcast(K * V * beta)
    bc_denominator_nk_or_beta = spark.sparkContext.broadcast(K * V * beta + nk.toArray.sum)
    bc_mh_steps = spark.sparkContext.broadcast(mh_steps)

    old_nkv = nkv.copy
    this
  }

  /**
    * 这里会比较骚
    * 因为是分布式，所以，每个partition上，z, nkv, ndk, nk的更新是同步进行的。
    * 所以，每个partition上，都记录了z, nkv, ndk, nk的改动。
    * 然后，在所有partition都计算完成后，再把z, nkv, ndk, nk的改动shuffle起来，然后统计、并重新boardcast改动后的z, nkv, ndk, nk
    * 这里会导致一个问题：对于每个partition而言，在一次迭代中，事实上都只是改变了自己这部分的数据
    * 这也许会导致难以收敛
    *
    * 这里做了一个近似：词的采样表，并没有时时刻刻都严格的保证词要从属于她被规定的类别中。
    * -   在doc proposal阶段，如果t从alpha table中采样，有可能会被分入不是该词的类别中。
    * -   进一步的，这会影响到nkv等的取值
    * -   该问题已经解决。
    *
    */
  def iter(): Unit = {
    val denominator_word_proposal = BV.apply[Double](bc_nk.value.toArray.map(ele => ele.toDouble + bc_V_beta.value))
    //    println("beta table length:" + denominator_word_proposal.map(ele => bc_beta.value / ele).length)
    val beta_table = new AliasSampler().build(denominator_word_proposal.toArray.map(ele => bc_beta.value / ele))
    //生成所有词的AliasSampler
    val tmp_nkv = bc_nkv.value
    old_nkv = tmp_nkv.copy
    val special_class = bc_special_class.value
    var words_table = ArrayBuffer[AliasSampler]()
    for (v <- 0 until V) {
      var topics = (for (k <- 0 until K) yield tmp_nkv(k, v))
        .zipWithIndex.filter(_._1 != 0).map(_._2) //取出该词的非0类别的编号
      if (special_class.contains(v))
        topics = topics.filter(ele => special_class(v).contains(ele)) //处理特殊的词
      val posi = for (k <- topics) yield tmp_nkv(k, v) / denominator_word_proposal(k)
      //      if (index_word(v) == "内存") {
      //        println(index_word(v) + "\twords table length:" + posi.length)
      //        println("nkv:" + (for (k <- 0 until K) yield tmp_nkv(k, v)).map(_.toString).reduce(_ + "," + _))
      //      }
      if (posi.nonEmpty)
        words_table += new AliasSampler().build_sparse(posi.toArray, topics.toArray)
      else
        words_table += new AliasSampler //完全就是空的，因为根本就不存在这个词。
    }

    val bc_words_table = spark.sparkContext.broadcast(words_table)
    val bc_beta_table = spark.sparkContext.broadcast(beta_table)


    val (adj_z, adj_nkv, adj_ndk, adj_nk) = docs.sample(false, sub_sample, new Random().nextLong())
      .mapPartitions {
        each_partition =>
          var local_z = bc_z.value
          var local_nkv = bc_nkv.value.copy
          var local_ndk = bc_ndk.value.copy
          var local_nk = bc_nk.value.copy
          val local_V_beta = bc_V_beta.value
          val local_beta = bc_beta.value
          val local_words_table = bc_words_table.value
          val local_beta_tabel = bc_beta_table.value
          //        val ran = new Random()


          var adj_z = Map[Int, Array[Int]]()


          for (document <- each_partition) {

            val d = document._1 //文档的编号
            val w_d = document._2 //一个文档中，所有词的编号
            val n_d = w_d.length
            adj_z += (d -> local_z(d)) //将z进行备份，塞进来
            for (i <- 0 until n_d) {
              val w = w_d(i) //词的编号

              for (step <- 0 until bc_mh_steps.value) {
                val old_topic = adj_z(d)(i)
                var s = old_topic //用来记录mh采样中的主题变迁
                //word proposal
                val sample_nk_or_beta = new Random().nextDouble() * bc_denominator_nk_or_beta.value

                var t = if (sample_nk_or_beta < bc_denominator_part_beta_nk_or_beta.value)
                  if (bc_special_class.value.contains(w))
                    local_beta_tabel.limit_sample(bc_special_class.value(w))
                  else
                    local_beta_tabel.sample()
                else local_words_table(w).sample()
                if (t != s) {
                  val nsw = local_nkv(s, w)
                  val ntw = local_nkv(t, w)
                  val ns = local_nk(s)
                  val nt = local_nk(t)
                  var nsd_alpha = local_ndk(d, s) + bc_alpha.value
                  var ntd_alpha = local_ndk(d, t) + bc_alpha.value
                  var nsw_beta = nsw + bc_beta.value
                  var ntw_beta = ntw + bc_beta.value
                  var ns_V_beta = ns + bc_V_beta.value
                  var nt_V_beta = nt + bc_V_beta.value

                  val proposal_nominator = nsw_beta * nt_V_beta
                  val proposal_denominator = ntw_beta * ns_V_beta
                  if (s == old_topic) {
                    nsd_alpha -= 1
                    nsw_beta -= 1
                    ns_V_beta -= 1
                  }
                  if (t == old_topic) {
                    ntd_alpha -= 1
                    ntw_beta -= 1
                    nt_V_beta -= 1
                  }
                  val pi_nominator = ntd_alpha * ntw_beta * ns_V_beta * proposal_nominator
                  val pi_denominator = nsd_alpha * nsw_beta * nt_V_beta * proposal_denominator
                  val pi = pi_nominator / pi_denominator //接受率
                  if (new Random().nextDouble() <= pi)
                    s = t
                }
                //doc proposal
                val sample_nd_or_alpha = new Random().nextDouble() * (n_d + bc_sum_alpha.value)

                t = if (n_d > sample_nd_or_alpha)
                  if (bc_special_class.value.contains(w)) {
                    val avail = bc_special_class.value(w)
                    val filter_z = adj_z(d).filter(ele => avail.contains(ele))
                    filter_z(new Random().nextInt(filter_z.length))
                  }
                  else adj_z(d)(math.floor(sample_nd_or_alpha).toInt)

                else if (bc_special_class.value.contains(w))
                  bc_special_class.value(w)(new Random().nextInt(bc_special_class.value(w).length)) //保证取样范围
                else bc_alpha_table.value.sample()
                if (t != s) {
                  val nsd = local_ndk(d, s)
                  val ntd = local_ndk(d, t)
                  var nsd_alpha = nsd + bc_alpha.value
                  var ntd_alpha = ntd + bc_alpha.value
                  var nsw_beta = local_nkv(s, w) + local_beta
                  var ntw_beta = local_nkv(t, w) + local_beta
                  var ns_V_beta = local_nk(s) + local_V_beta
                  var nt_V_beta = local_nk(t) + local_V_beta

                  val proposal_nominator = nsd_alpha
                  val proposal_denominator = ntd_alpha

                  if (s == old_topic) {
                    nsd_alpha -= 1
                    nsw_beta -= 1
                    ns_V_beta -= 1
                  }
                  if (t == old_topic) {
                    ntd_alpha -= 1
                    ntw_beta -= 1
                    nt_V_beta -= 1
                  }
                  val pi_nominator = ntd_alpha * ntw_beta * ns_V_beta * proposal_nominator
                  val pi_denominator = nsd_alpha * nsw_beta * nt_V_beta * proposal_denominator
                  val pi = pi_nominator / pi_denominator
                  if (new Random().nextDouble() <= pi)
                    s = t
                }
                //update topic
                if (s != old_topic) {
                  val word_assigned_topic = adj_z(d)
                  word_assigned_topic(i) = s
                  adj_z += (d -> word_assigned_topic) //更新adj_z

                  local_nkv(old_topic, w) -= 1
                  local_ndk(d, old_topic) -= 1
                  local_nk(old_topic) -= 1

                  local_nkv(s, w) += 1
                  local_ndk(d, s) += 1
                  local_nk(s) += 1
                }
                //              if (index_word(w) == "内存")
                //                println(index_word(w) + "\t" + (for (k <- 0 until K) yield local_nkv(k, w)).map(_.toString).reduce(_ + "," + _))
              }
            }
          }
          val adj_nkv = (bc_nkv.value - local_nkv).toDenseMatrix
          val adj_ndk = (bc_ndk.value - local_ndk).toDenseMatrix
          val adj_nk = bc_nk.value - local_nk
          Iterator((adj_z, adj_nkv, adj_ndk, adj_nk))
      }
      .reduce {
        (ele1, ele2) =>
          val z = ele1._1 ++ ele2._1
          val nkv = ele1._2 + ele2._2
          val ndk = ele1._3 + ele2._3
          val nk = ele1._4 + ele2._4
          (z, nkv, ndk, nk)
      }
    //把从各个节点综合的结果，广播出去
    val z = bc_z.value
    val nkv = bc_nkv.value
    val ndk = bc_ndk.value
    val nk = bc_nk.value
    bc_z = spark.sparkContext.broadcast(z ++ adj_z)
    bc_nkv = spark.sparkContext.broadcast(nkv.toDenseMatrix - adj_nkv)
    bc_ndk = spark.sparkContext.broadcast(ndk.toDenseMatrix - adj_ndk)
    bc_nk = spark.sparkContext.broadcast(nk - adj_nk)
  }

  /**
    * 从nkv中，提取出有用的信息。
    * 主要就是计算新的nkv和旧的nkv之间的相似性了
    */
  def summary(): Double = {
    val new_nkv = bc_nkv.value
    println(s"变动项：${(new_nkv - old_nkv).valuesIterator.filter(_ != 0).length}个")
    val total_words = bc_denominator_nk_or_beta.value - bc_denominator_part_beta_nk_or_beta.value
    var sum_cos = 0.0
    var is_show = true
    for (v <- 0 until V) {
      val word_amount = static_nv(v)
      val old_vec = for (k <- 0 until K) yield old_nkv(k, v)
      val new_vec = for (k <- 0 until K) yield new_nkv(k, v)

      val old_length = math.sqrt(old_vec.map(ele => ele * ele).sum)
      val new_length = math.sqrt(new_vec.map(ele => ele * ele).sum)
      val weighted_cos = old_vec.zip(new_vec).map(ele => ele._2 * ele._1).sum / old_length / new_length * word_amount
      if (old_length != 0 && new_length != 0)
        sum_cos += weighted_cos

      //      if (old_length == 0 && is_show) {
      //        is_show = false
      //        println(v)
      //        println(old_vec.map(_.toString).reduce(_ + " " + _))
      //        println(new_vec.map(_.toString).reduce(_ + " " + _))
      //        println(old_length, new_length)
      //        println(weighted_cos, word_amount)
      //        println(old_vec.zip(new_vec).map(ele => ele._2 * ele._1).sum)
      //      }
    }
    val final_cos = sum_cos / total_words
    //    println(sum_cos, total_words)
    final_cos
  }

  def describ_model(take: Int): Unit = {
    //    println(bc_nkv.value)
    //    println(bc_ndk.value)
    //    println(bc_nk.value)
    for (k <- 0 until K) {
      var words_arr = for (v <- 0 until V) yield
        if (static_nv(v) == 0 || bc_nkv.value(k, v) == 0)
          (v, 0.0)
        else
          (v, (1.0 / K + bc_nkv.value(k, v).toDouble / static_nv(v)) * math.log10(bc_nkv.value(k, v)))
      words_arr = words_arr.sortWith((ele1, ele2) => ele1._2 > ele2._2)
      println(s"第${k}类：" + words_arr.take(take).map(ele => index_word(ele._1)).reduce(_ + "," + _))
    }
    //    for (d <- 0 until D) {
    //      val doc_vec = for (k <- 0 until K) yield bc_ndk.value(d, k)
    //      val word_sum = doc_vec.sum
    //      var has_topic = false
    //      doc_vec.foreach(ele => if (ele > word_sum * 0.3) has_topic = true)
    ////      if (has_topic)
    ////        println(s"文档编号：${d}\t文档主题情况：${doc_vec.map(_.toString).reduce(_ + " " + _)}")
    //    }
  }

  def fit(): Unit = {
    //    println(bc_nkv.value)
    //    println(bc_ndk.value)
    //    println(bc_nk.value)
    val tmr_start = System.currentTimeMillis()
    for (i <- 0 until iter_times) {
      println(s"开始第${i}次迭代")
      iter()
      if (i % 10 == 0) {
        val similarity = summary()
        println(s"第${i}次迭代后，变动率为${(100 - similarity * 100).formatted("%.2f")}%。")
      }
    }
    describ_model(20)
    val tmr_end = System.currentTimeMillis()
    println(s"总耗时${((tmr_end - tmr_start) / 1000.0).formatted("%.2f")}s")
  }

  /**
    * 最终决定，把整个模型写下来算了。
    * 包括：z,Documents
    * 决定还是分开储存了。rdd单独储存，对读取都比较方便
    * 2018-7-9改动：不再保存自定义类别的信息，因为本来就有。但是，类别的序号信息依旧需要保存
    * 2018-7-10修正：懒得存整个模型了，估计光是nkv就够用了
    *
    * @param fw
    */
  def save(fw: BufferedWriter, rdd_path: String): Unit = {
    //    //第一步，储存rdd
    //    docs.map {
    //      line =>
    //        line._1.toString + "\t" + (if (line._2.nonEmpty) line._2.map(_.toString).reduce(_ + "," + _) else "")
    //    }.repartition(1).saveAsTextFile(rdd_path)
    fw.write(s"${D}\t${V}\t${bc_special_class.value.size}\t${index_tag.size}\r\n")
    //先输出nkv
    val nkv = bc_nkv.value
    for (v <- 0 until V) {
      val write_str = (for (k <- 0 until K) yield nkv(k, v)).map(_.toString).reduce(_ + "," + _)
      fw.write(write_str + "\r\n")
    }
    //然后是word_index
    index_word.foreach {
      line =>
        val write_str = line._1.toString + "\t" + line._2
        fw.write(write_str + "\r\n")
    }
    //自定义类的序号
    index_tag.foreach {
      line =>
        val write_str = line._1.toString + "\t" + line._2
        fw.write(write_str + "\r\n")
    }
    fw.close()
  }

  /**
    *
    * @param br
    */
  def load_old_model(br: BufferedReader): Unit = {
    var line = br.readLine()
    var strs = line.split("\\t")
    D = strs(0).toInt
    V = strs(1).toInt
    val specail_class_size = strs(2).toInt
    val index_tag_size = strs(3).toInt
    println(D, V, specail_class_size, index_tag_size)
    //读入nkv
    line = br.readLine()
    strs = line.split(",")
    K = strs.length
    val nkv = BM.zeros[Int](K, V)
    for (k <- 0 until K)
      nkv(k, 0) = strs(k).toInt
    for (v <- 1 until V) {
      line = br.readLine()
      strs = line.split(",")
      for (k <- 0 until K)
        nkv(k, v) = strs(k).toInt
    }
  }
}
