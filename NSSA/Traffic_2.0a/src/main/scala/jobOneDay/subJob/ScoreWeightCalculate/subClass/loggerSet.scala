package jobOneDay.subJob.ScoreWeightCalculate.subClass

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2018/1/17.
  */
trait loggerSet {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
