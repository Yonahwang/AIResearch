package jobFiveMinute.subJob.AbnormalPorts.AbnormalPortsClass

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2018/6/22.
  */
trait LoggerSupport {
  @transient lazy val logger_ap = Logger.getLogger(this.getClass)
}
