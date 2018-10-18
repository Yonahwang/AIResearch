package jobOneHour.subJob.trafficAnomaly.classmethod

import org.apache.log4j.Logger

/**
  * Created by Yiutto on 2018年1月17日 0017.
  */
trait writeToLogger {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
