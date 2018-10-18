package jobFiveMinute.subJob.AssetAutoFind.subClass

import org.apache.log4j.Logger
/**
  * Created by Administrator on 2018/6/14.
  */
trait LoggerSupport {
  @transient lazy val logger_assetsfind = Logger.getLogger(this.getClass)
}
