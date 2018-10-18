package jobOneHour.subClass

import java.io.{File, FileWriter}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger


/**
  * Created by Administrator on 2018/6/14.
  */
class methodReadData(spark: SparkSession, properties: Properties, logger:Logger) {

  //读取HDFS文件(读取所有文件夹)
  def readDataFromHDFS(data_source:String="netflow", hour:Int = -1) = {

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
    val file_without_last = file.filter(_ != file(file.length - 1))
    hdfs.close()

    //判断是否处理过这些文件
//    val path_read = properties.getProperty("file.have.read."+data_source)
    var last_hour = getHourTime(hour) //获取前一小时的时间
    logger.error("正在处理文件："+ last_hour)
    var result_data:DataFrame = null    //存储最终数据
        val file_last_hour = file_without_last.filter {
          x =>
            x.split("/").last.substring(0, 10) == last_hour
        }        //过滤出前一小时的文件目录

    //判断要读取的数据文件数是否非0
    logger.error("文件数："+ file_last_hour.length)
    if(file_last_hour.length == 0){
      last_hour = null
      logger.error("没有要处理的文件")
    }
    else {
      try{
        logger.error("正在处理文件")
        val dataList = file_last_hour.map {
          line =>
            val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> line)
            spark.read.options(options).format("com.databricks.spark.csv").load()
        }
        logger.error("批量读取原始数据文件成功")
        //合并所有结果
        val union_function = (a: DataFrame, b: DataFrame) => a.union(b).toDF
        val data_list_not_null = dataList.filter { each => each != null }.filter(_.columns.length >= 1)
        result_data = if (data_list_not_null.length > 1) data_list_not_null.tail.foldRight(data_list_not_null.head)(union_function) else data_list_not_null.head
        logger.error("合并结果成功")
      } catch {
        case e: Exception => logger.error(e.getMessage)
      }
    }

    (last_hour, result_data)
  }


  def getHourTime(hour:Int)={
    val cal = Calendar.getInstance
    cal.add(Calendar.HOUR, hour)
    val time: Date = cal.getTime
    val newtime: String =new SimpleDateFormat("yyyyMMddHH").format(time)
    newtime
  }

  //写文件
  def wirte_file(content:String, path:String, add:Boolean = false) = {
    val file = new File(path).exists()  //判断条件
    if(!file){  //不存在则新建文件
      new File(path).createNewFile()
    }
    val fileW = new FileWriter(path, add)
    fileW.write(content)
    fileW.close()
  }

  //读文件
  def read_file(filePath: String): String = {
    import scala.io.Source
    val file = new File(filePath)
    var str = ""
    if(file.exists()){  //判断文件是否存在
    // 将文件的内容保存为字符串数组
    val strArr = Source.fromFile(filePath).getLines().toArray
      str = strArr.head
    }
    str
  }




}
