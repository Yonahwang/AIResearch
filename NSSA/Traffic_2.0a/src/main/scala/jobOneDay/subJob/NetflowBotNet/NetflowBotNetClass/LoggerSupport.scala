package jobOneDay.subJob.NetflowBotNet.NetflowBotNetClass

import org.apache.log4j.Logger

/**
  * Created by TTyb on 2017/9/27.
  */
trait LoggerSupport {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
