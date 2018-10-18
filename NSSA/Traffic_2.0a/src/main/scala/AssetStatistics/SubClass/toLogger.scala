package AssetStatistics.SubClass

import org.apache.log4j.Logger

/**
  * Created by yiutto on 2018年7月25日 0025.
  */
trait toLogger {
  @transient lazy val logger = Logger.getLogger(this.getClass)
}
