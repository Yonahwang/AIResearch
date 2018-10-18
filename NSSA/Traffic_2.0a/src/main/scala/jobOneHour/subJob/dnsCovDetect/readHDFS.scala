package jobOneHour.subJob.dnsCovDetect

import java.util.Properties

import jobOneHour.subClass.LoggerSupport
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

object readHDFS extends LoggerSupport {



  def read(spark: SparkSession,properties: Properties)={

    //    获取hdfs的dns数据目录
    val InputFile = properties.getProperty("hdfs.path") + properties.getProperty("dnsTaskDet.InputFile")

    //    读取文件夹内按时间字段排序最近时间的12个文件
    val fileNames12: Array[Long] = get12FileName(InputFile)
    //      fileNames12.foreach(println)

    //    经过滤的待处理文件名
    val filteredFileNames: Array[Long] = clean12FileName(spark,fileNames12,properties)

    //    将数据合并为一个完整的dataframe
    val data: DataFrame = rawData(spark, filteredFileNames, InputFile)
    data.printSchema()
    data
  }

  /**
    * 获取目录下按时间字段排序最近时间的12个文件名
    * @param dirs   hdfs上存DNS数据的目录
    * @return
    */
  def get12FileName(dirs: String): Array[Long] ={
    //    获取目录下的所有文件名（完整路径）
    val con = new Configuration()
    val uri = new java.net.URI(dirs)
    val hdfs = FileSystem.get(uri, con)
    val p = new Path(dirs)
    val fs: Array[FileStatus] = hdfs.listStatus(p)
    val filePath: Array[Path] = FileUtil.stat2Paths(fs)

    //    取最近时间的12个文件名（中的时间字符串）
    //    必须先取13个最近的文件，然后再去掉最近的文件，因为最近的文件可能还在读写状态
    val files = filePath.map{
      line =>
        line.getName.replace(dirs,"").substring(0,12).toLong
    }.sortWith((a, b)=>a>b).take(3).sortWith((a,b)=>a<b).take(2)

    logger.error("get 12 file names finished")
    logger.error(files(0))
    files
  }

  /**
    * 去掉已经处理过的文件名，保证每一个文件仅处理一次
    *
    * @param spark
    * @param nearestNames12   目录中最近的12个文件名（时间字符串）
    * @return 返回本次待处理的文件名
    */
  def clean12FileName(spark: SparkSession, nearestNames12: Array[Long],properties:Properties): Array[Long] ={
    //    获得已处理过的文件名（对应最大时间值的文件名）
    val visitedFileNames = properties.getProperty("visitTag")
    var visited = List("0")
    try{
      //      println(visitedFileNames)
      visited = spark.sparkContext.textFile(visitedFileNames).collect().toList
      visited
    }catch{
      case e:AnalysisException =>{
        println("read visitedFileNames, file Path does not exist:")
        println(e)
        visited
      }

      case e:Exception =>{
        println("read visitedFileNames, fail to load files:")
        println(e)
        visited
      }
    }
    //    val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> visitedFileNames)
    //    var visited = spark.sparkContext.parallelize(Seq("")).toDF("name")
    //    try{
    //      visited = spark.read.format("com.databricks.spark.csv").options(options).load().toDF("name").persist()
    //      visited
    //    }catch{
    //      case e:AnalysisException =>{
    //        println("read visitedFileNames, file Path does not exist:")
    //        println(e)
    //        visited
    //      }
    //
    //      case e:Exception =>{
    //        println("read visitedFileNames, fail to load files:")
    //        println(e)
    //        visited
    //      }
    //
    //    }
    //
    //
    //        hdfs目录下最近的12个文件名
    //    val files12: DataFrame = spark.sparkContext.parallelize(nearestNames12).toDF("name")
    //    val files12: DataFrame = spark.sparkContext.parallelize(nearestNames12).toDF("name")
    //    //    12个文件名中未被处理的文件名
    //    val cleanFileNames: Dataset[Row] = files12.except(visited)
    //    返回本次待处理的文件名
    //    println("visited(0)")
    //    println(visited(0))
    //    println("nearestNames12")
    //    nearestNames12.foreach(println)
    val cleanFileNames = nearestNames12.filter(i => (i > visited(0).toLong) )
//    println("cleanFileNames")
//    cleanFileNames.foreach(println)
    cleanFileNames
  }


  /**
    * 根据文件名读取数据
    *
    * @param spark
    * @param fileNames  dataframe的"name"列存储文件名（中的时间字符串）
    * @param dirs  hdfs上存DNS数据的目录
    * @return
    */
  def rawData(spark: SparkSession, fileNames: Array[Long], dirs: String) = {
    val p = new Path(dirs)
    //    fileNames.show()
    //    val filesNamesArray: Array[String] = fileNames.select("name").rdd.map(_.toString()).collect()

    //    每次读一个文件返回一个dataframe
    val time1 = System.currentTimeMillis()
    val dfs = fileNames.map{
      line =>
        import spark.implicits._
        var data = spark.sparkContext.parallelize(Seq("")).toDF()
        try{
          val fileName = p.toString + "/" + line + ".txt"
          val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> fileName)
          data = spark.read.format("com.databricks.spark.csv").options(options).load().persist()
//          println(data.count())
//          println(fileName)
//          data.printSchema()
          data
        }catch{
          case e:Exception =>{
            println("ERROR occured when we try to read 12 files, try to skip this file")
            println(e)
            data
          }
        }
        data
    }.filter(_!=null)
    val time2 = System.currentTimeMillis()
    logger.error("读数据用时" + (time2 - time1) / 1000 + "秒")

    //    12个dataframe（或者更少）合并成一个dataframe
    val unionFun = (a: DataFrame, b: DataFrame) => a.union(b).toDF
    dfs.toList.foreach(println)
    val data = dfs.tail.foldRight(dfs.head)(unionFun)
    println("union  ",data.count())
    data
  }

  /**
    * 将本次已经处理的文件名中对应时间最近的文件名存入visitTag
    *
    * @param spark
    * @param filteredFileNames  本次已经处理的文件名
    */
  def saveVisited(spark: SparkSession,filteredFileNames: Array[Long],properties: Properties )={
    val visitedFileNames = properties.getProperty("visitTag")
    import spark.implicits._
    //    filteredFileNames是df
    //    val maxTimeFile = filteredFileNames.select("name").rdd.map(_.toString().toLong).collect.sortWith((a,b)=> a>b)
    //    filteredFileNames是Array
    val maxTimeFile = filteredFileNames.sortWith((a,b)=> a>b).take(1)
    try{
      spark.sparkContext.parallelize(maxTimeFile).toDF("name").coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(visitedFileNames)
    }catch{
      case e:Exception =>{
        println(e)
        println("save Visited file name failed")
      }
    }
  }
}
