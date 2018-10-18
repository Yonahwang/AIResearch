package jobOneDay.subJob.Botnet.subClass

import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sql._

/**
  * Created by Administrator on 2018/1/17.
  */
trait readMethod extends loggerSet{
  def getESDate(spark: SparkSession, properties: Properties) = {
    //es查询语句
    val es_index = properties.getProperty("es.data.index")
    val query_mail =
      s"""{
         |  "query":{
         |    "filtered":{
         |      "query":{
         |        "match_all":{}
         |      },
         |      "filter":{
         |        "terms":{"appproto":["dns"]}
         |      }
         |    }
         |  }
         |}""".stripMargin
    val rowDF = spark.esDF(es_index, query_mail)
    val data = rowDF.select("srcip", "dstip", "srcport", "dstport", "appproto", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10")
      .toDF("srcip", "dstip", "srcport", "dstport", "protocol", "flags_response", "flags_reply_code", "queries_name", "Queries.type", "Answers.name", "Answers.type", "Answers.ttl", "Answers.datalength", "Answers.address", "association_id")
    //      .filter("appproto = 'dns' and col2 = '3'")
    logger.error("读取ES最近一天数据成功>>>")
    logger.error("共有数据量：" + data.count())
    data.persist(StorageLevel.MEMORY_AND_DISK_SER)
    data
  }

  def readLocal(properties: Properties, spark: SparkSession) = {
    import spark.implicits._
    val data = spark.sparkContext.textFile("./conf/test.txt").map { row =>
      val temp = row.split("\t")
      (temp(0), temp(5), temp(6), temp(7))
    }.toDF("srcip", "flags_response", "flags_reply_code", "queries_name")
    logger.error("读取本地数据成功>>>")
    logger.error("共有数据量：" + data.count())
    //    data.show(5)
    data
  }

  //读取HDFS文件
  def readDataFromHDFS(spark: SparkSession, properties: Properties) = {
    //获取hdfs文件
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(properties.getProperty("hdfs.netflow.path")), conf)
    val fileList: Array[FileStatus] = hdfs.listStatus(new Path(properties.getProperty("hdfs.netflow.path")))
    //获取hdfs目录文件(过滤最后一个文件)
    val file = fileList.map(_.getPath.toString).sorted
    val fileWithoutLast = file.filter(_ != file(file.length - 1))
    hdfs.close()
    //读取所有文件数据
    val dataList = fileWithoutLast.map{
      line =>
        val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> line)
        spark.read.options(options).format("com.databricks.spark.csv").load()
    }
    //    val dataList = fileWithoutLast.map(spark.read.json(_))
    //合并所有结果
    val unionFunction = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val dataListNotNull = dataList.filter { each => each != null }.filter(_.columns.length >= 1)
    val dataListUnion = if (dataListNotNull.length > 1) dataListNotNull.tail.foldRight(dataListNotNull.head)(unionFunction) else dataListNotNull.head
    logger.error("原始数据")
    dataListUnion
  }

}
