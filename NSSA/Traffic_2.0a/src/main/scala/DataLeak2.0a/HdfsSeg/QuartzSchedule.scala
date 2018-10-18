package SituationAwareness_2_0a.HdfsSeg

import java.io.{File, FileInputStream}
import java.util.{Date, Properties}

import org.quartz.impl.StdSchedulerFactory
import org.quartz.{CronScheduleBuilder, JobBuilder, TriggerBuilder}

object QuartzSchedule {
  var hdfs_seg_cron = ""
  var hdfs_seg_offset: Long = 0

  def load_Properties(): Unit = {
    val properties = new Properties()
    val path = new File(new File(".").getAbsolutePath + "/conf/troublesome.properties").getAbsolutePath //这个是用在服务器上的？
    println(path)
    //    val path = "conf/troublesome.properties"
    properties.load(new FileInputStream(path))
    hdfs_seg_cron = properties.getProperty("hgh.hdfs_seg.schedule")
    hdfs_seg_offset = properties.getProperty("hgh.hdfs_seg.offset").toLong

  }

  def standard_int(n: Int): String = {
    var res = n.toString
    if (res.length == 1)
      res = "0" + res
    res
  }

  def main(args: Array[String]): Unit = {
    load_Properties()
    //获取时间
    val current_timestamp = System.currentTimeMillis()
    val target_timestamp = current_timestamp - hdfs_seg_offset
    var target_period = new Date()
    target_period.setTime(target_timestamp)
    val year = target_period.getYear + 1900
    val month = target_period.getMonth + 1
    val day = target_period.getDate
    var hour = target_period.getHours.toString
    //    println(target_period)
    //    println(year, month, day, hour)
    var date = year.toString + standard_int(month) + standard_int(day)
    //定义定时任务
    val scheduler = StdSchedulerFactory.getDefaultScheduler
    val cron_trigger = TriggerBuilder.newTrigger
      .withIdentity("hdfs_seg_trigger", "group_hgh")
      .startNow()
      .withSchedule(CronScheduleBuilder
        .cronSchedule(hdfs_seg_cron))
      .build()

    val job = JobBuilder.newJob(new MajorMethods().getClass)
      .withIdentity("hdfs_seg_job", "group_hgh") //定义job的名字，和所属的组。job名字不能重复
      .usingJobData("date", date) //传入k,v对，可以重复调用来传一堆东西
      .usingJobData("hour", hour)
      .usingJobData("offset", hdfs_seg_offset.toString)
      .build()
    scheduler.scheduleJob(job, cron_trigger)
    scheduler.start()


  }
}
