package jobFiveMinute.subClass

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2018/6/12.
  */
trait LoggerSupport {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
