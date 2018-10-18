package jobFiveMinute.subJob.netflowProcess.classmethod

import org.apache.log4j.Logger

/**
  * Created by Yiutto on 2018年1月17日 0017.
  */
trait writeToLogger {
  @transient lazy val logger_np = Logger.getLogger(this.getClass)
}
