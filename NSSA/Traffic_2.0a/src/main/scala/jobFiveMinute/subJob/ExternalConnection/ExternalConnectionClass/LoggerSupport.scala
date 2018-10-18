package jobFiveMinute.subJob.ExternalConnection.ExternalConnectionClass

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2018/6/22.
  */
trait LoggerSupport {
  @transient lazy val logger_ec = Logger.getLogger(this.getClass)
}
