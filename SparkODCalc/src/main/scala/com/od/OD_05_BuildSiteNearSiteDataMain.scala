package com.od

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.xml.XML
import com.od.OD_01_PreprocessData._

object OD_05_BuildSiteNearSiteDataMain {
  val spark=SparkSession
    .builder()
    .appName("OD_05_BuildSiteNearSiteDataMain")
    //.master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  //spark.conf.set("spark.sql.shuffle.partitions",200)
  //spark.sql.autoBroadcastJoinThreshold = -1
  //不限定小表的大小
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  // 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值
  spark.conf.set("spark.sql.join.preferSortMergeJoin", true)
  spark.conf.set("spark.dynamicAllocation.enabled", "false")
  println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

  //公交站点、轨道站点、站点临近站点、历史记录路径从resources中的config进行获取
  var jarWholePath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
  try jarWholePath = java.net.URLDecoder.decode(jarWholePath, "UTF-8")
  catch {
    case e: Exception =>
      System.out.println(e.toString)
  }
  val jarPath = new File(jarWholePath).getParentFile.getAbsolutePath
  val xmlPath = jarPath + File.separator + "config.xml"
  val xml = XML.load(xmlPath)
  val mode = (xml \ "mode").text

  if(mode=="BR") {
    val busSitePath = (xml \ "baseData" \ "busSitePath").text
    val railSitePath = (xml \ "baseData" \ "railSitePath").text
    val siteNearSitePath = (xml \ "tempData" \ "siteNearSitePath").text
    val busSiteData = preprocessBusSiteData(spark, busSitePath)
    val railSiteData= preprocessRailSiteData(spark,railSitePath)
    val routeSite= mergeBusAndRailSiteData(spark,busSiteData,railSiteData)
    val siteNearSite=buildSiteNearSiteData(spark,routeSite,busSiteData).distinct()
    siteNearSite.write.mode(SaveMode.Overwrite).option("header","true").csv(siteNearSitePath)
  }
  else if(mode=="B") {
    val busSitePath = (xml \ "baseData" \ "busSitePath").text
    val busSiteData = preprocessBusSiteData(spark, busSitePath)
    val siteNearSitePathWithOnlyBusSite = (xml \ "resultData" \ "siteNearSitePathWithOnlyBusSite").text
    val routeSite=busSiteData
    val siteNearSite = buildSiteNearSiteData(spark, routeSite, busSiteData).distinct().toDF()
    siteNearSite.write.mode(SaveMode.Overwrite).option("header", "true").csv(siteNearSitePathWithOnlyBusSite)
  }
  spark.stop()
}
