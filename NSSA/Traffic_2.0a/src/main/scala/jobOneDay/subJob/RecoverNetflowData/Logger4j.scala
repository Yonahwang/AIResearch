package jobOneDay.subJob.RecoverNetflowData

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2018年6月28日 0028.
  */
trait Logger4j {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
