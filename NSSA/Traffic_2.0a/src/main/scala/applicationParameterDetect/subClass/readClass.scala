package applicationParameterDetect.subClass

import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by Administrator on 2018/5/7.
  */
class readClass(spark: SparkSession, sql:SQLContext,properties: Properties) extends LoggerSet{


  //从HDFS读取数据
  def getDataFromHDFSByMerge() = {

    //获取hdfs文件
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(properties.getProperty("hdfs.netflow.path")), conf)
    val fileList: Array[FileStatus] = hdfs.listStatus(new Path(properties.getProperty("hdfs.netflow.path")))
    //获取hdfs目录文件(过滤最后一个文件)
    val file = fileList.map(_.getPath.toString).sorted
    val fileWithoutLast = file.filter(_ != file(file.length - 1))
    //不在历史已处理过的列表中的文件
    //    val newFile = fileWithoutLast.diff(filePast).toList
    hdfs.close()
    //      logger.error("111")
    //      println(newFile)
    //      try {
    //读取所有文件数据
    val dataList = fileWithoutLast.map{
      line =>
        var data:DataFrame = null
        try{
          val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> line)
          data = sql.read.options(options).format("com.databricks.spark.csv").load()
//          data = spark.read.json(line)
//          data.show(5)
        }
        catch {
          case e: Exception => logger.error("Reading false: " + line)
        }
        data
    }
    //合并所有结果
    val unionFunction = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    val dataListNotNull = dataList.filter{each=> each != null}.filter(_.columns.length >= 1)
    val dataListUnion = if (dataListNotNull.length > 1) dataListNotNull.tail.foldRight(dataListNotNull.head)(unionFunction) else dataListNotNull.head
    logger.error("原始数据")
    dataListUnion

  }


  //从本地获取文件
  def getDataFromLocal(spark:SparkSession) = {
    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> "./conf/app_data.csv")
    val data = sql.read.options(options).format("com.databricks.spark.csv").load()
    data
  }



}
