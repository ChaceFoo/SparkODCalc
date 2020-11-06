package com.od

import java.io.{File, UnsupportedEncodingException}

import com.od.OD_01_PreprocessData.{buildSiteNearSiteData, dealGpsData, mergeBusAndRailSiteData, preprocessBusICData, preprocessBusSiteData, preprocessRailICData, preprocessRailSiteData}
import com.od.OD_02_MatchGetOnAndGetDownSite.{buidHistroyData, gainGetOnSite, matchGetDownSiteDataFail, matchGetDownSiteDataSuccess, matchGetOffSiteWithNullData, matchGetOnSite, matchGetOnSiteWithRailIC, newHistroyData, travelChain}
import com.od.OD_03_ChangeDataAndSingleODData.{getChangeData, getChangeType, getSingleODData}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.xml.XML

object OD_06_Deal11Month {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OD_06_Deal11Month")
      //.master("local[4]")  //提交到集群需注释掉
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
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
      case e: UnsupportedEncodingException =>
        System.out.println(e.toString)
    }
    val jarPath = new File(jarWholePath).getParentFile.getAbsolutePath
    val xmlPath = jarPath + File.separator + "Deal201911Config.xml"
    val xml = XML.load(xmlPath)
    val baseStr="2019-11-"
    for(i<-1 to 30) {
      val str=baseStr+String.format("%d",i)
      val busSitePath = (xml \ "baseData" \ "busSitePath").text+File.separator+str
      val railSitePath = (xml \ "baseData" \ "railSitePath").text+File.separator+str
      val siteNearSitePath = (xml \ "tempData" \ "siteNearSitePath").text+File.separator+str
      val historyPath = (xml \ "tempData" \ "historyPath").text+File.separator+str
      val busICPath = (xml \ "initData" \ "busICPath").text+File.separator+str
      val railICPath = (xml \ "initData" \ "railICPath").text+File.separator+str
      val gpsDataPath = (xml \ "initData" \ "gpsDataPath").text+File.separator+str
      val allODResultPath = (xml \ "resultData" \ "allODResultPath").text+File.separator+str
      val singleODResultPath = (xml \ "resultData" \ "singleODResultPath").text+File.separator+str
      val changeResultPath = (xml \ "resultData" \ "changeResultPath").text+File.separator+str

      val _busICData = preprocessBusICData(spark, busICPath)
      val gpsData = dealGpsData(spark, gpsDataPath)
      val busICData = gainGetOnSite(spark, _busICData, gpsData) //利用gps数据匹配公交IC的上车站点号，初始站点可靠的情况下可以省略这一步，直接使用PreprocessBusICData的结果作为busICData
      val busSiteData = preprocessBusSiteData(spark, busSitePath)
      val railSiteData = preprocessRailSiteData(spark, railSitePath)
      var siteNearSiteData: DataFrame = null
      try {
        siteNearSiteData = spark.read.format("csv").option("header", "true").load(siteNearSitePath)
        println("siteNerSiteData存在，使用现有siteNerSiteData")
      }
      catch {
        case e: Exception => {
          val routeSite = mergeBusAndRailSiteData(spark, busSiteData, railSiteData)
          println("siteNerSiteData不存在，创建siteNerSiteData")
          val siteNearSite = buildSiteNearSiteData(spark, routeSite, busSiteData).distinct().toDF()
          siteNearSite.write.mode(SaveMode.Overwrite).option("header", "true").csv(siteNearSitePath)
          siteNearSiteData = spark.read.format("csv").option("header", "true").load(siteNearSitePath)
        }
      }
      val matchGetOnSiteData = matchGetOnSite(spark, busICData, busSiteData)
      val railICData = preprocessRailICData(spark, railICPath, railSitePath)
      val matchGetOnSiteWithRailICData = matchGetOnSiteWithRailIC(spark, matchGetOnSiteData, railICData)
      val travelChainData = travelChain(spark, matchGetOnSiteWithRailICData)
      val matchGetOffSiteWithNull = matchGetOffSiteWithNullData(spark, travelChainData, siteNearSiteData)
      val matchGetDownSiteDataSuccessData = matchGetDownSiteDataSuccess(spark, matchGetOffSiteWithNull)
      val weightData = newHistroyData(spark, matchGetDownSiteDataSuccessData, historyPath)
      val matchGetDownSiteDataFailData = matchGetDownSiteDataFail(spark, matchGetOffSiteWithNull, weightData)
      val allData = matchGetDownSiteDataSuccessData.union(matchGetDownSiteDataFailData)
      val allData2 = allData.selectExpr("cardNo", "cardType", "getOnTime", "getOnBusNo", "getOnRouteID", "getOnRoute", "getOnSiteID", "getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude", "getOffRoute", "getOffSite", "changeType", "value1", "value2", "getOfftime", "nextTime")
      val result = allData2.join(busSiteData,
        allData2("getOffRoute") === busSiteData("route") and
          allData2("getOffSite") === busSiteData("site"))
        .selectExpr("cardNo", "cardType", "getOnTime", "getOnBusNo", "getOnRouteID", "getOnRoute", "getOnSiteID", "getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude",
          "getOffTime", "routeID as getOffRouteID", "getOffRoute", "siteID as getOffSiteID", "getOffSite", "siteName as getOffSiteName", "longitude as getOffLongitude", "latitude as getOffLatitude", "changeType", "value1", "value2")
      val ODResult = result.union(railICData)
      val allODData = getChangeType(spark, ODResult)
      val changeODData = getChangeData(spark, ODResult)
      val singleODData = getSingleODData(spark, ODResult)
      allODData.write.mode(SaveMode.Overwrite).option("header", "true").csv(allODResultPath)
      changeODData.write.mode(SaveMode.Overwrite).option("header", "true").csv(changeResultPath)
      singleODData.write.mode(SaveMode.Overwrite).option("header", "true").csv(singleODResultPath)
    }
    spark.stop()
  }
}
