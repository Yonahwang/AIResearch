package applicationResponseDetect.subClass

import org.apache.log4j.Logger

/**
  * Created by Administrator on 2017/12/26.
  */
trait LoggerSet {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
