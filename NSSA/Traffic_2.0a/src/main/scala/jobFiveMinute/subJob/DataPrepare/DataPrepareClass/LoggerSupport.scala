package jobFiveMinute.subJob.DataPrepare.DataPrepareClass

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2018/6/22.
  */
trait LoggerSupport {
  @transient lazy val logger_dp = Logger.getLogger(this.getClass)
}
