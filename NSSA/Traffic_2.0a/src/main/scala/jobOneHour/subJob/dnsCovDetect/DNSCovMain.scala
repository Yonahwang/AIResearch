package jobOneHour.subJob.dnsCovDetect

import java.util.Properties

import jobOneHour.subClass.LoggerSupport
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

class DNSCovMain(properties: Properties, spark: SparkSession, dnsData: DataFrame) extends LoggerSupport {
  def main(): Unit ={
  val index = properties.getProperty("dnsTask.index")
   if (index == "predict"){
       val time1 = System.currentTimeMillis()
       new RFModelDetect().detect(properties, spark, dnsData)
       val time2 = System.currentTimeMillis()
     logger.error("检测用时为" + (time2 - time1) / 1000 + "秒")
   }
   else{
         val time1 = System.currentTimeMillis()
         new RFModelTrain(properties).train()
         val time2 = System.currentTimeMillis()
     logger.error("训练用时为" + (time2 - time1) / 1000 + "秒")
     }

 }
}
