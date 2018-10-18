package jobOneDay.subClass

import java.net.URI
import java.util.Properties
import org.apache.log4j.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}


/**
  * Created by Administrator on 2018/6/14.
  */
class methodReadData(spark: SparkSession, properties: Properties, logger:Logger) {

  //读取HDFS文件(读取所有文件夹)
  def readDataFromHDFS(data_source:String="netflow") = {

    //创建hdfs路径
    var hdfs_path = properties.getProperty("hdfs.path") + properties.getProperty("hdfs.clean.data.path")
    if(data_source == "netflow"){
      hdfs_path = hdfs_path + "netflow/"
    }
    if(data_source == "dns"){
      hdfs_path = hdfs_path + "dns/"
    }
    if(data_source == "http"){
      hdfs_path = hdfs_path + "http/"
    }

    //获取hdfs文件目录
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(hdfs_path), conf)
    val fileList: Array[FileStatus] = hdfs.listStatus(new Path(hdfs_path))

    //获取hdfs目录文件(过滤最后一个文件)
    val file = fileList.map(_.getPath.toString).sorted
    val fileWithoutLast = file.filter(_ != file(file.length - 1))
    hdfs.close()

    //获取前一天的日期
    var last_day = getDayTime(-1)

    //过滤出前一天的文件目录
    val file_without_last_day = fileWithoutLast.filter{
      x =>
        x.split("/").last.substring(0, 8) == last_day
    }
    println("前一天文件数："+file_without_last_day.length)

    var result_data:DataFrame = null    //存储最终数据
    if(file_without_last_day.length == 0){
      last_day = null
    }
    else {
      try {
        //读取所有文件数据
        val data_list = file_without_last_day.map {
          line =>
            val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> line)
            spark.read.options(options).format("com.databricks.spark.csv").load()
        }
        //合并所有结果
        val union_function = (a: DataFrame, b: DataFrame) => a.union(b).toDF
        val data_list_not_null = data_list.filter { each => each != null }.filter(_.columns.length >= 1)
        result_data = if (data_list_not_null.length > 1) data_list_not_null.tail.foldRight(data_list_not_null.head)(union_function) else data_list_not_null.head
      } catch {
        case e: Exception => logger.error(e.getMessage)
      }
    }

    (last_day, result_data)
  }


  def getDayTime(day:Int)={
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, day)
    val time: Date = cal.getTime
    val newtime: String =new SimpleDateFormat("yyyyMMdd").format(time)
    newtime
  }



}
