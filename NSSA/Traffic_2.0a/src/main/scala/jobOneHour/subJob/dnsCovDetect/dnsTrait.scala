package jobOneHour.subJob.dnsCovDetect

import java.io.File

import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.log4j.{Logger, PropertyConfigurator}

import scala.util.Try

//trait dnsTrait {
////  本地环境
//
//  val filePath = Try(System.getProperty("user.dir")).getOrElse(new File("..").getAbsolutePath)
//  PropertyConfigurator.configure(filePath + "/conf/log4j_DNS.properties")
//  @transient lazy val config:Configuration = new PropertiesConfiguration(filePath + "/conf/DNSChannel.properties")
//
//  @transient lazy val log = Logger.getLogger(this.getClass)
//  log.error("start the whole dns Detection")
//
//}
