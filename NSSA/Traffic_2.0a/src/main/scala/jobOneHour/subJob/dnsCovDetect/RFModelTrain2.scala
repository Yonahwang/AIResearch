package jobOneHour.subJob.dnsCovDetect

import java.util.Properties

object RFModelTrain2  {

  def train(properties: Properties){
     val algorithms = Array("a","b")
     val Result = algorithms.par.map{
       alg =>
         val R = try{
           val path = properties.getProperty("dnsTaskDet.model" + "/" + alg)
           val r = RFModelTrainScheduler.train()
             r
         }catch{
           case e =>{
             throw new Exception("Something Wrong!", e)
             ("null",0.0)
           }
         }
         (alg, R)
     }.toArray
  }
}
