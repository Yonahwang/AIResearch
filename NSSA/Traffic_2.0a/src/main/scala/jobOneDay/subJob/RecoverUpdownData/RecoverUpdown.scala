package jobOneDay.subJob.RecoverUpdownData

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException

class RecoverUpdown(spark: SparkSession, properties: Properties) extends LoggerSupport {

    def recoverupdown() {
      try {
        val hdfsURL = properties.getProperty("hdfs.path")
        val targetpath = properties.getProperty("hdfs.target.path")
        val hdfsdata = (getEStableIndex().toLong - 1).toString
        val wholepath = hdfsURL + targetpath + "thostattacks_" + hdfsdata + ".txt"
        val data = spark.read.json(wholepath)

        //过滤出上下行异常流量 且target字段为false的，存进postgre
        val fdata = data.filter("target == false").filter("modeltype == '上下行流量异常'")
        //        fdata.show(3,false)
        val groupudf = udf((recordtime: String) => {
          val d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(recordtime.toLong).substring(10, 16).replace(":", ".").toDouble
          if (d >= 0.00 && d < 9.0) "早上上班前"
          else if (d >= 9.00 && d < 12.0) "早上上班时间"
          else if (d >= 12.0 && d < 14.0) "中午休息时间"
          else if (d >= 14.0 && d < 18.0) "下午上班时间"
          else if (d >= 18.0 && d < 24.0) "晚上休息时间"
          else ""
        })

        val ffdata = fdata.withColumn("group", groupudf(fdata("resulttime")))
        //        ffdata.show(4,false)
        saveTOTemp(ffdata, spark)


      } catch {
        case e: EsHadoopIllegalArgumentException => logger_ud.error(e.getMessage)
        case e: Exception => logger_ud.error(e.getMessage)
      }
    }

  //存到临时表
  def saveTOTemp(data: DataFrame, spark: SparkSession) {
    //存结果表
    val url = properties.getProperty("postgre.address")
    val user = properties.getProperty("postgre.user")
    val ps = properties.getProperty("postgre.password")
    val table = properties.getProperty("postgre.table.name.train")
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).getTime

    data.foreachPartition {
      part =>
        Class.forName("org.postgresql.Driver")
        val conn = DriverManager.getConnection(url, user, ps)
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

  //获取ES当天索引
  def getEStableIndex(): String = {
    val cal = Calendar.getInstance //实例化Calendar对象
    cal.add(Calendar.DATE, 0)
    val tablelong: String = new SimpleDateFormat("yyyyMMdd").format(cal.getTime) //设置格式并且对时间格式化
    tablelong
  }

  //生成数字索引
  def genaralROW(): String = {
    var row = ((Math.random * 9 + 1) * 100000).toInt + "" //六位随机数
    row += new Date().getTime / 1000
    row
  }

}

