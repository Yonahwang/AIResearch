package jobFiveMinute.subJob.Updown

import java.io.{File, Serializable}
import java.net.{InetAddress, URI}
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.spark.sql._
import jobFiveMinute.subClass._
import org.apache.hadoop.fs.FileStatus
import org.apache.log4j.{Logger, PropertyConfigurator}

import scala.util.Try


class UpdownPredict(spark:SparkSession,data:DataFrame,properties: Properties) extends LoggerSupport with Serializable with saveToKAFKA{

  def UpdownPredictMain(): Unit = {
    /**
      * 每5分钟都判断临时表里是否有未知流量的ip，比较流量基线，插入到es
      */

    PropertyConfigurator.configure(Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath) + "/conf/jobFiveMinute.properties")
    try {
      logger_ud.error("-----------开始训练上下行流量异常-------------")

      /**
        * 判断hdfs地址是否为空，如果为空，取原始数据，如果不为空，取前一天hdfs文件
        */

      if (ifExistHdfsAddress()==false){
        //如果为0,即没有文件在该地址下,读取hdfs的文件构建基线存到postgre

        val options = Map("header" -> "true", "delimiter" -> "\t", "path" -> getFileName())
        val data = spark.read.format("com.databricks.spark.csv").options(options).load()
        //取5分钟数据训练
        val trainData = traindata(spark,data)
        trainData.show(3,false)
        //存到postgre
        saveTOTemp(trainData,spark)
      }// else {
//        //即为有文件在该地址下，直接取昨天的hdfs表
//        val hdfsURL = properties.getProperty("hdfs.path")
//        val targetpath = properties.getProperty("hdfs.target.path")
//        val hdfsdata = (getEStableIndex().toLong-1).toString
//        val wholepath = hdfsURL+targetpath+"thostattacks_"+hdfsdata+".txt"
//        val data = spark.read.json(wholepath)
////        data.show(3,false)
//        //过滤出上下行异常流量 且target字段为false的，存进postgre
//        val fdata = data.filter("target == false").filter("modeltype == '上下行流量异常'")
////        fdata.show(3,false)
//        val groupudf = udf((recordtime: String) => {
//          val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordtime.toLong).substring(10, 16).replace(":", ".").toDouble
//          if (d >= 0.00 && d < 9.0) "早上上班前"
//          else if (d >= 9.00 && d < 12.0) "早上上班时间"
//          else if (d >= 12.0 && d < 14.0) "中午休息时间"
//          else if (d >= 14.0 && d < 18.0) "下午上班时间"
//          else if (d >= 18.0 && d < 24.0) "晚上休息时间"
//          else ""
//        })
//
//        val ffdata = fdata.withColumn("group",groupudf(fdata("resulttime")))
////        ffdata.show(4,false)
//        saveTOTemp(ffdata,spark)
//
//      }
      logger_ud.error("-----------开始预测上下行流量异常-------------")
      //临时表
      val tempdata = getDataFromPostgre(spark, properties.getProperty("postgre.address"),properties.getProperty("postgre.user"), properties.getProperty("postgre.password"), properties.getProperty("postgre.table.name.train")).filter("type == '上下行流量异常'")
      //获取最后倒数第二个文件的时间，作为输出结果表的字段
      val dateFileName = getFileNamedate()

      //读取es访问过可疑域名的表
      val domaintablename = "thostattacks_" + getEStableIndex() + "/thostattacks_" +getEStableIndex()
      val domaindata = getDataFromES(domaintablename, spark).filter("modeltype == '可疑域名'").select("ip").distinct()

      //处理-检测
      val (normaldata,dealData) = dealdata(spark, data,domaindata,tempdata,dateFileName)
      dealData.show(4,false)


      //正常数据存postgre
      saveTOTemp1(normaldata,spark)

      //存kafka
      toKafka(spark,(properties.getProperty("kafka.nodes"),properties.getProperty("kafka.topic2")),dealData)

    } catch {
      case e: EsHadoopIllegalArgumentException => logger_ud.error(e.getMessage)
      case e: Exception => logger_ud.error(e.getMessage)
    }
  }

  //处理-检测
  def dealdata(spark:SparkSession,data:DataFrame,domaindata:DataFrame,tempdata:DataFrame,dateFileName:String): (DataFrame,DataFrame) ={

    // 过滤访问过可疑域名的ip
    val filterdomaindata = data.filter("flagIP == '1'").select("srcip", "dstip", "srcport", "dstport", "protocol", "upbytesize", "downbytesize", "starttime").orderBy("starttime")
    //判断未知流量中的ip是否在临时表中
    val intersectdata = tempdata.select("srcip").intersect(filterdomaindata.select("srcip"))
    //取出临时表中的记录
    val tempintersectdata = intersectdata.join(tempdata, intersectdata("srcip") === tempdata("srcip"), "left").drop(intersectdata("srcip"))
    val transtempintersect = tempintersectdata.select(
      tempintersectdata("srcip"),
      tempintersectdata("typeresult"),
      tempintersectdata("baselineresult").cast("Double"))
    transtempintersect.createOrReplaceTempView("tempdata")
    //取出临时表中最大阈值的记录
    val maxtempintersectdata = spark.sql(
      """select srcip,typeresult,max(baselineresult) as maxbase from tempdata group by srcip,typeresult
      """.stripMargin).filter("maxbase != 'Infinity' and maxbase != 'NaN'")
    //取出未知流量中有临时表的记录
    val intersectjoinfilter = intersectdata.join(filterdomaindata, intersectdata("srcip") === filterdomaindata("srcip"), "left").drop(intersectdata("srcip"))//.filter("recordtime != ''")

    val groupudf = udf((recordtime: String) => {
        val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordtime.toLong * 1000).substring(10, 16).replace(":", ".").toDouble
        if (d >= 0.00 && d < 9.0) "早上上班前"
        else if (d >= 9.00 && d < 12.0) "早上上班时间"
        else if (d >= 12.0 && d < 14.0) "中午休息时间"
        else if (d >= 14.0 && d < 18.0) "下午上班时间"
        else if (d >= 18.0 && d < 24.0) "晚上休息时间"
        else ""
    })


    val groupdata = intersectjoinfilter.withColumn("group", groupudf(intersectjoinfilter("starttime")))//.filter("group != ''")
    //计算比例
    val updownudf = udf((upflow:Double,downflow:Double)=>if(downflow == 0.0) upflow else upflow/downflow)
    val udfdata = groupdata.withColumn("bili", updownudf(groupdata("upbytesize"), groupdata("downbytesize"))).filter("bili != 'Infinity' and bili != 'NaN'")
    //和临时表做拼接
    val joindata = maxtempintersectdata.join(udfdata, maxtempintersectdata("srcip") === udfdata("srcip") &&
      maxtempintersectdata("typeresult") === udfdata("group"), "inner").drop(maxtempintersectdata("srcip"))
      .drop(maxtempintersectdata("typeresult"))
    println("..................")
    val compareudf = udf((bili:Double,threshold:Double)=> (if(bili>threshold) "异常" else "正常"))

    val judgedata = joindata.withColumn("judge", compareudf(joindata("bili"), joindata("maxbase")))//.filter("judge == '异常'")
    val abnormaldata = judgedata.filter("judge == '异常'")
    val normaldata = judgedata.filter("judge == '正常'")

    //前端时间段
    val timeudf = udf((recordtime: String) => {
      val date = new SimpleDateFormat("yyyy-MM-dd-").format(new Date())
      val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordtime.toLong*1000).substring(10, 16).replace(":", ".").toDouble
      if (d >= 0.0 && d < 4.0) date+"0:00"   //-14:00
      else if (d >= 4.0 && d < 8.0) date+"4:00"   //-13:00
      else if (d >= 8.0 && d < 12.0) date+"8:00"   //-12:00
      else if (d >= 12.0 && d < 16.0) date+"12:00"   //-11:00
      else if (d >= 16.0 && d < 20.0) date+"16:00"    //-10:00
      else if (d >= 20.0 && d < 24.0) date+"20:00"   //-9:00
      else ""
    })
    val timedata = abnormaldata.withColumn("standby01",timeudf(abnormaldata("starttime"))).filter("standby01 != ''")


    val evidenceudf = udf((srcip: String, recordtime: String, bili: Double, maxbase: Double) =>
      ("资产" + srcip + "在" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordtime.toLong * 1000) + "的上下行流量异常，上下行流量比例为:"
        + bili.formatted(("%.2f")) + "超过了流量基线：" + maxbase.formatted(("%.2f"))))
    //生成证据字段
    val evidencedata = timedata.withColumn("evidence", evidenceudf(timedata("srcip"), timedata("starttime"), timedata("bili"), timedata("maxbase")))

    import spark.implicits._
    val endf = evidencedata.select($"srcip", $"dstip", $"srcport", $"dstport", $"protocol", $"evidence", $"maxbase", $"bili", $"upbytesize", $"downbytesize", $"starttime", $"group", $"standby01").map {
      case Row(srcip: String, dstip: String, srcport: String, dstport: String, protocol: String, evidence: String, maxbase: Double, bili: Double, upbytesize: String, downbytesize: String, starttime: String, group: String, standby01: String) =>
        TORow(genaralROW(), srcip, "上下行流量异常", starttime.toLong, evidence, srcip + "," + dstip + "," + srcport + "," + dstport + "," + protocol, 0, srcip, dstip, upbytesize.toString, downbytesize.toString, bili.formatted("%.2f").toString, maxbase.formatted("%.2f").toString, standby01, dateFileName,protocol)
    }.toDF("id", "ip", "modeltype", "resulttime", "evidence", "eventsource", "happen", "normaltime", "abnormaltime", "abnormalport", "abnormalproto", "abnormaltype", "hisbaseline", "standby01","standby02","standby03")
//    endf.show(5,false)

    //[共有字段:]  五元组  模型类型  发生时间  上报ip    [模型特有字段:]  连接间隔  连接次数
    val original_logudf = udf(
      (srcip: String, dstip: String,
       modeltype: String, resulttime: Long,
       up: String,down:String,updown:String,history:String) =>
        ("{\"网元IP\":\"" + srcip + "\"," +
          "\"目的IP\":\"" + dstip + "\"," +
          "\"源端口\":\"" + "" + "\"," +
          "\"目的端口\":\"" + "" + "\"," +
          "\"协议\":\"" + "" + "\"," +
          "\"模型类型\":\"" + modeltype + "\"," +
          "\"发生时间\":\"" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(resulttime*1000) + "\"," +
          "\"上报IP\":\"" + InetAddress.getLocalHost().getHostAddress + "\"," +
          "\"上行流量\":\"" +up+ "\"," +
          "\"下行流量\":\"" +down+ "\"," +
          "\"上下行流量比例\":\"" +updown+ "\"," +
          "\"历史阈值\":\"" +history+ "\"}"))


    val original_logdata = endf.withColumn("original_log",original_logudf(endf("normaltime"), endf("abnormaltime"),
      endf("modeltype"), endf("resulttime"),endf("abnormalport"),endf("abnormalproto"),endf("abnormaltype"),endf("hisbaseline")))


    //加一个可疑域名的标签
    val domaindataSet = domaindata.distinct().rdd.map(x => x.toString()).collect().toSet
    val judgeDomainudf = udf((ip: String) => {
      val daS = domaindataSet.contains(ip)
      if (daS == true) "1"
      else "0"
    })
    val judgeThreadAndDomian = original_logdata.withColumn("standby02", judgeDomainudf(original_logdata("ip")))

    val tlsudf =udf((proto7:String)=>(if(proto7 == "tls") "加密流量" else "非加密流量" ))
    //关联原始数据得到目的经纬度以及省份，国家，城市
    val selectdata = data.select("srcip","dstip","dstport","protocol","proto7","slatitude","slongitude","scountry","sprovince","scity","dcountry","dprovince","dcity","dlatitude","dlongitude")
    val eventdata = judgeThreadAndDomian.join(selectdata,judgeThreadAndDomian("normaltime")===selectdata("srcip")&&judgeThreadAndDomian("abnormaltime")===selectdata("dstip")&&judgeThreadAndDomian("abnormalproto")===selectdata("dstport"),"left")
    val tlsdata = eventdata.withColumn("victimtype",tlsudf(eventdata("proto7")))
    tlsdata.printSchema()

    import spark.implicits._
    val lastdata = tlsdata.select($"id", $"ip", $"modeltype", $"resulttime", $"evidence", $"eventsource", $"happen", $"normaltime", $"abnormaltime", $"abnormalport", $"abnormalproto", $"abnormaltype", $"hisbaseline", $"standby01",$"standby02",$"standby03",$"victimtype", $"original_log",
      $"slatitude", $"slongitude", $"scountry", $"sprovince", $"scity", $"dcountry", $"dprovince", $"dcity", $"dlatitude", $"dlongitude").distinct().rdd.map {
      line =>
      val id = line.getAs[String]("id")
      val ip = line.getAs[String]("ip")
      val modeltype = line.getAs[String]("modeltype")
      val resulttime = line.getAs[Long]("resulttime")*1000
      val evidence = line.getAs[String]("evidence")
      val eventsource = line.getAs[String]("eventsource")
      val happen = line.getAs[Int]("happen")
      val normaltime = line.getAs[String]("normaltime")
      val abnormaltime = line.getAs[String]("abnormaltime")
      val abnormalport = line.getAs[String]("abnormalport")
      val abnormalproto = line.getAs[String]("abnormalproto")
      val abnormaltype = line.getAs[String]("abnormaltype")
      val hisbaseline = line.getAs[String]("hisbaseline")
      val standby01 = line.getAs[String]("standby01")
      val standby02 = line.getAs[String]("standby02")
      val standby03 = line.getAs[String]("standby03")
      val victimtype = line.getAs[String]("victimtype")
      val original_log = line.getAs[String]("original_log")
      val slatitude = line.getAs[String]("slatitude")
      val slongitude = line.getAs[String]("slongitude")
      val scountry = line.getAs[String]("scountry")
      val sprovince = line.getAs[String]("sprovince")
      val scity = line.getAs[String]("scity")
      val dcountry = line.getAs[String]("dcountry")
      val dprovince = line.getAs[String]("dprovince")
      val dcity = line.getAs[String]("dcity")
      val dlatitude = line.getAs[String]("dlatitude")
      val dlongitude = line.getAs[String]("dlongitude")

        eventRes(id, ip, modeltype, resulttime, evidence, eventsource, happen,normaltime, abnormaltime, abnormalport, abnormalproto,abnormaltype, hisbaseline, standby01,standby02,standby03,
          "MODEL_UPDOWN_MV1.0_001_0011", standby03, InetAddress.getLocalHost().getHostAddress, "AttackBasic6B11",
          scountry + "#" + sprovince + "#" + scity + "#" + slatitude + "#" + slongitude + "#" + dcountry + "#" + dprovince + "#" + dcity + "#" + dlatitude + "#" + dlongitude,
          original_log, victimtype, "0",normaltime, abnormaltime,eventsource.split(",")(2),eventsource.split(",")(3))

    } .toDF("id","ip", "modeltype", "resulttime", "evidence", "eventsource", "happen", "normaltime", "abnormaltime", "abnormalport", "abnormalproto",
      "abnormaltype", "hisbaseline", "standby01", "standby02", "standby03", "event_rule_id", "proto", "reportneip", "event_sub_type", "position",
      "original_log", "encrypted", "event_flag","srcip","dstip","srcport","dstport")
    (normaldata,lastdata)
  }


  def getDataFromPostgre(spark: SparkSession,jdbcurl:String,user:String,password:String,tablename:String):DataFrame ={
    //连接数据库
    val connectionProperties = new Properties()
    try {
      connectionProperties.put("user", user)
      connectionProperties.put("password",password )
      connectionProperties.put("driver", "org.postgresql.Driver")
    } catch {
      case e: Exception => {
        logger_ud.error(s"连接postgre数据库失败！！！" + e.getMessage)
      }
    }
    val data = spark.read.jdbc(jdbcurl,tablename,connectionProperties)
    data
  }


  //生成数字索引
  def genaralROW():String = {
    var row = ((Math.random * 9 + 1) * 100000).toInt + "" //六位随机数
    row += new Date().getTime / 1000
    row
  }


  //获取倒数第二个文件的日期 201805151535
  def getFileNamedate(): String = {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

    var fileName = ""
    //获取前缀后缀
    var prefix = ""
    var suffix = ""
    var arrayFile: List[Long] = List()
    val filePath = properties.getProperty("hdfs.netflow.path")
    val hdfsUrl = properties.getProperty("hdfs.path")
    val hadfPath = hdfsUrl + "/" + filePath
    val output = new Path(hadfPath)
    val hdfs = FileSystem.get(new java.net.URI(hdfsUrl), new Configuration())
    val fs = hdfs.listStatus(output)
    val filepath = FileUtil.stat2Paths(fs)
    hdfs.close()
    filepath.foreach { eachfile =>
      val eachFileName = eachfile.getName.split("\\.")

      prefix = eachFileName.head.replace(hadfPath, "").replace("/netflow", "")
      suffix = eachFileName.last
      arrayFile = arrayFile :+ prefix.toLong
    }
    val str = arrayFile.sorted.init.last.toString

    var startstr = str.substring(8,10)+":"+str.substring(10,12)
    //转成时间戳，再加5分钟，再转换为时间
    val strformat =  new SimpleDateFormat("yyyyMMddHHmm").parse(str).getTime()+300000
    val laststr = new SimpleDateFormat("HH:mm").format(new Date(strformat))
    startstr+"-"+laststr
  }

  //获取ES当天索引
  def getEStableIndex():String={
    val cal = Calendar.getInstance //实例化Calendar对象
    cal.add(Calendar.DATE, 0)
    val tablelong: String = new SimpleDateFormat("yyyyMMdd").format(cal.getTime) //设置格式并且对时间格式化
    tablelong
  }

  //读取ES数据
  def getDataFromES(tablename: String, spark: SparkSession): DataFrame = {
    //    val query = "{\"query\":{\"range\":{\"recordtime\":{\"gte\":{\"lte\":}}}}}"
    val rawDF =  spark.esDF(tablename)
    rawDF
  }

  //存到临时表
  def saveTOTemp(data:DataFrame,spark:SparkSession){
    //存结果表
    val url = properties.getProperty("postgre.address")
    val user = properties.getProperty("postgre.user")
    val ps = properties.getProperty("postgre.password")
    val table = properties.getProperty("postgre.table.name.train")
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).getTime

    data.foreachPartition{
      part =>
        Class.forName("org.postgresql.Driver")
      val conn = DriverManager.getConnection(url,user,ps)
        part.foreach {
          line =>
            try {
              val sqlString = s"insert into $table(id,type,ip,srcip,typeresult,baselineresult,standby01) values (?,?,?,?,?,?,?)"
              val prep = conn.prepareStatement(sqlString)
              prep.setString(1, genaralROW())
              prep.setString(2, "上下行流量异常")
              prep.setString(3, line.getAs[String]("srcip"))
              prep.setString(4, line.getAs[String]("srcip"))
              prep.setString(5, line.getAs[String]("group"))
              prep.setString(6, line.getAs[Double]("threshold").toString)
              prep.setString(7, time.toString)
              prep.executeUpdate
            } catch {
              case e: Exception => logger_ud.error("导入出错" + e.getMessage)
            }
            finally {}
        }
        conn.close
    }
  }

  //从hdfs上取正常的数据存到临时表
  def saveTOTemp1(data:DataFrame,spark:SparkSession){
    //存结果表
    val url = properties.getProperty("postgre.address")
    val user = properties.getProperty("postgre.user")
    val ps = properties.getProperty("postgre.password")
    val table = properties.getProperty("postgre.table.name.train")
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).getTime

    data.foreachPartition{
      part =>
        Class.forName("org.postgresql.Driver")
        val conn = DriverManager.getConnection(url,user,ps)
        part.foreach {
          line =>
            try {
              val sqlString = s"insert into $table(id,type,ip,srcip,typeresult,baselineresult,standby01) values (?,?,?,?,?,?,?)"
              val prep = conn.prepareStatement(sqlString)
              prep.setString(1, genaralROW())
              prep.setString(2, "上下行流量异常")
              prep.setString(3, line.getAs[String]("srcip"))
              prep.setString(4, line.getAs[String]("srcip"))
              prep.setString(5, line.getAs[String]("group"))
              prep.setString(6, line.getAs[Double]("bili").toString)
              prep.setString(7, time.toString)
              prep.executeUpdate
            } catch {
              case e: Exception => logger_ud.error("导入出错" + e.getMessage)
            }
            finally {}
        }
        conn.close
    }
  }


  def getFileName(): String = {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
    //    val config = new LogSupport().config
    var fileName = ""
    //获取前缀后缀
    var prefix = ""
    var suffix = ""
    var arrayFile: List[Long] = List()
    val filePath = properties.getProperty("hdfs.netflow.path")
    val hdfsUrl = properties.getProperty("hdfs.path")
    val hadfPath = hdfsUrl + "/" + filePath
    val output = new Path(hadfPath)
    val hdfs = FileSystem.get(new java.net.URI(hdfsUrl), new Configuration())
    val fs = hdfs.listStatus(output)
    val filepath = FileUtil.stat2Paths(fs)
    hdfs.close()
    filepath.foreach { eachfile =>
      val eachFileName = eachfile.getName.split("\\.")

      prefix = eachFileName.head.replace(hadfPath, "").replace("/netflow", "")
      suffix = eachFileName.last
      arrayFile = arrayFile :+ prefix.toLong
    }
    fileName = hadfPath + "/" + arrayFile.sorted.init.last.toString + "." + suffix
    fileName
  }


  def ifExistHdfsAddress():Boolean={
    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, Path}

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val con = new Configuration()
    val hdfsURL = properties.getProperty("hdfs.path")
    val targetpath = properties.getProperty("hdfs.target.path")
    val hdfsdata = (getEStableIndex().toLong-1).toString

    val wholepath = hdfsURL+targetpath
    val txtpath = hdfsURL+targetpath+"thostattacks_"+hdfsdata+".txt"

//    val wholepath = "hdfs://10.130.10.41:9000/spark/target/"
//    val txtpath = ""


    val txturi = new java.net.URI(txtpath)
    val wholeuri = new java.net.URI(wholepath)

    val txthdfs = FileSystem.get(txturi, con)
    val wholehdfs = FileSystem.get(wholeuri, con)

    val txtp = new Path(txtpath)
    val wholep = new Path(wholepath)

    if (txthdfs.exists(txtp)==true && wholehdfs.exists(wholep)==true) true
    else if (txthdfs.exists(txtp)==false && wholehdfs.exists(wholep)==true) true  //地址存在，文件不存在，即没有生成昨天的hdfs文件
    else if (txthdfs.exists(txtp)==false && wholehdfs.exists(wholep)==false) false
    else false
  }

  def traindata(spark: SparkSession,data:DataFrame):DataFrame={
    val groupudf = udf((recordtime: String) => {
      val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordtime.toLong*1000).substring(10, 16).replace(":", ".").toDouble
      if (d >= 0.00 && d < 9.0) "早上上班前"
      else if (d >= 9.00 && d < 12.0) "早上上班时间"
      else if (d >= 12.0 && d < 14.0) "中午休息时间"
      else if (d >= 14.0 && d < 18.0) "下午上班时间"
      else if (d >= 18.0 && d < 24.0) "晚上休息时间"
      else ""
    })
    val updownudf = udf((upflow:Double,downflow:Double)=>(upflow/downflow))

    val filterdomaindata = data.filter("flagIP == '1'").select("srcip", "dstip", "srcport", "dstport", "protocol", "upbytesize", "downbytesize", "recordtime").orderBy("recordtime")
    val groupdata = filterdomaindata.withColumn("group", groupudf(filterdomaindata("recordtime"))).filter("group != ''")
    //计算比例
    val udfdata = groupdata.withColumn("bili", updownudf(groupdata("upbytesize"), groupdata("downbytesize"))).filter("bili != 'Infinity' and bili != 'NaN'")
    udfdata.printSchema()
    //按照分组，找出每组中最大的比例
    udfdata.createOrReplaceTempView("udfdata")
    val sdata = spark.sql(
      """select srcip,group,max(bili) as threshold from udfdata group by srcip,group""".stripMargin)
      .filter("threshold != 'Infinity' and threshold != 'NaN'")
    sdata.printSchema()
    sdata
  }


}

case class TORow(id: String, ip: String, modeltype: String = "上下行流量异常", resulttime: Long, evidence: String, eventsource: String, happen: Int = 0, normaltime: String, abnormaltime: String, abnormalport: String, abnormalproto: String, abnormaltype: String, hisbaseline: String, standby01: String, standby02: String, standby03: String) extends Serializable {}

case class eventRes(id: String, ip: String, modeltype: String = "上下行流量异常", resulttime: Long, evidence: String, eventsource: String,
                    happen: Int = 0, normaltime: String, abnormaltime: String, abnormalport: String, abnormalproto: String,
                    abnormaltype: String, hisbaseline: String, standby01: String, standby02: String, standby03: String,
                    event_rule_id: String, proto: String, reportneip: String, event_sub_type: String, position: String,
                    original_log: String, encrypted: String, event_flag: String,srcip:String,dstip:String,srcport:String,dstport:String)


