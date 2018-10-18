package jobFiveMinute.subClass

import java.io.{File, FileWriter}
import java.net.URI
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession, _}

import scala.io.Source
import scala.util.Try




/**
  * Created by Administrator on 2018/6/12.
  */
class methodReadData(spark:SparkSession, properties: Properties) {

  def readOriginHDFS(read_origin_data:Boolean = true, file_num:Int = 2) = {

    //获取hdfs文件
    val conf = new Configuration()
    var path_netflow = properties.getProperty("hdfs.path") + properties.getProperty("hdfs.origin.data.path") + "netflow/"
    if(!read_origin_data){
      path_netflow = properties.getProperty("hdfs.path") + properties.getProperty("hdfs.clean.data.path") + "netflow/"
    }
    val hdfs = FileSystem.get(URI.create(path_netflow), conf)
    val fileList = hdfs.listStatus(new Path(path_netflow))

    //获取hdfs目录倒数第二个文件
    val fileListSort = fileList.map(_.getPath.toString).sorted
    var targetFile = Try(fileListSort(fileListSort.length-file_num)).getOrElse(null)  //倒数第2个
    hdfs.close()

    //判断是否处理过这些文件
    val path_read = properties.getProperty("file.have.read")
    val path_read_ori = properties.getProperty("file.have.read.ori")
    val file_read = if(read_origin_data) read_file(path_read_ori) else read_file(path_read)   //上次处理的文件
    var oriData:DataFrame = null
    println("正在处理文件："+ targetFile)
    if(targetFile == file_read || targetFile == null){
      targetFile = null //若是已经处理过的文件，则target file改为null
    }
    else{
      //读取文件数据
      if(read_origin_data) {
        oriData = spark.read.json(targetFile)
        wirte_file(targetFile, path_read_ori)//将本次处理的文件写入到本地文件
      }
      else{
        val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> targetFile)
        oriData = spark.read.options(options).format("com.databricks.spark.csv").load()
        wirte_file(targetFile, path_read)//将本次处理的文件写入到本地文件
      }
    }

    (targetFile, oriData)
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
