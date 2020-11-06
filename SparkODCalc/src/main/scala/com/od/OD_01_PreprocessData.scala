package com.od

import java.text.SimpleDateFormat
import java.util.Locale

import com.od.Helper.{GpsData, GpsData2, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.od.OD_02_MatchGetOnAndGetDownSite._

import scala.xml.XML

object OD_01_PreprocessData {
  //预处理公交IC数据
  def preprocessBusICData(spark: SparkSession, path: String): DataFrame = {
    //格式化时间yyyy-MM-dd HH:mm:ss转毫秒数，注册为udf
    spark.udf.register("toMsTime", (s: String) => {
      //2018-06-29 06:40:54.080000000
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", new Locale("en")).parse(
        s
      ).getTime
    })

    spark.udf.register("getValue", (s1: String,s2:String) => {
      if(s1=="3"|| s1=="14" || s1=="15")
        "0"
      else
        s2
    })

    val _busICData = spark.read.format("csv")
      .option("delimiter", "\t").option("header", "true").option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .load(path)
    val schemas = Seq("getOnTime", "x1", "cardNo", "cardType", "value1", "value2", "ChangeType", "x5", "x6", "getOnBusNo", "getOnRoute", "getOnSite")
    val BusICDataWithRename = _busICData.toDF(schemas: _*)
    val busICData = BusICDataWithRename.selectExpr("cardNo","lpad(cardType,2,0) as cardType", "toMsTime(getOnTime) as getOnTime", "lpad(getOnBusNo,8,0) as getOnBusNo", "lpad(getOnRoute,8,0) as getOnRoute", "lpad(getOnSite,6,0) as getOnSite", "ChangeType", "getValue(cardType,value1) as value1","getValue(cardType,value2) as value2")
    busICData
    //BusICDataWithRename.selectExpr("cardNo","toMsTime(getOnTime) as getOnTime","lpad(getOnBusNo,8,0) as getOnBusNo","lpad(getOnRoute,8,0) as getOnRoute","lpad(getOnSite,6,0) as getOnSite","ChangeType")
    //      case class NewIC(cardNo: String,ChangeRoute: String,ChangeTime: String,getOnCarTime: String,getOnBusNo: String,getOnRoute: String,getOnSiteSerial: String,getOnSiteName: String,upOrDown: String,getOnLongitude: String,getOnLatitude: String,getOffBusNo: String,getOffRoute: String,getOffSiteSerial: String,Route2: String,Site2: String)
  }
  //预处理轨道IC数据
  def preprocessRailICData(spark: SparkSession, railICPath: String, railSitePath: String): DataFrame = {
    import spark.implicits._
    //获取换乘类型
    spark.udf.register("gainChangetOffLongitudege", (s: String) => {
      //2018-06-29 06:40:54.080000000
      try {
        s.substring(16, 18)
      }
      catch {
        case e:Exception =>{
          "00"
        }
      }
    })

    //获取线路号
    spark.udf.register("gainRouteID", (s: String) => {
      //2018-06-29 06:40:54.080000000
      try {
        //2018-06-29 06:40:54.080000000
        "00000000000000" + s.substring(0, 2)
      }
      catch {
        case exception: NullPointerException => {
          "0000000000000000"
        }
      }
    })

    //获取线路号
    spark.udf.register("gainRoute", (s: String) => {
      //2018-06-29 06:40:54.080000000
      try {
        //2018-06-29 06:40:54.080000000
        "000000" + s.substring(0, 2)
      }
      catch {
        case exception: NullPointerException => {
          "00000000"
        }
      }
    })
    //获取站点号
    spark.udf.register("gainSite", (s: String) => {
      try {
        //2018-06-29 06:40:54.080000000
        "0000" + s.substring(2, 4)
      }
      catch {
        case exception: NullPointerException => {
          "000000"
        }
      }

    })
    //格式化时间yyyyMMddHHmmss转毫秒数，注册为udf
    spark.udf.register("toMsTime2", (s: String) => {
      //2018-06-29 06:40:54.080000000
      new SimpleDateFormat("yyyyMMddHHmmss", new Locale("en")).parse(
        s
      ).getTime
    })

    //格式化时间yyyyMMddHHmmss转毫秒数，注册为udf
    spark.udf.register("toMsTime2", (s: String) => {
      //2018-06-29 06:40:54.080000000
      new SimpleDateFormat("yyyyMMddHHmmss", new Locale("en")).parse(
        s
      ).getTime
    })

    //车号，轨道的辅助字段，用以和公交IC数据进行匹配
    spark.udf.register("gainBusNo", (s: String) => {
      "00000000"
    })

    //车号，轨道的辅助字段，用以和公交IC数据进行匹配
    spark.udf.register("gainBusNo", (s: String) => {
      "00000000"
    })

    //应扣金额
    spark.udf.register("value", (s1: String,s2: String) => {
      s1.toInt+s2.toInt
    })
    //"TICKET_ID","TXN_DATE","TXN_TIME","CARD_COUNTER","STL_DATE","FILE_ID","TICKET_MAIN_TYPE","TICKET_TYPE","MEDIA_TYPE","VER_NO","TICKET_CSN","TRANS_CODE","TXN_STATION_ID","LAST_STATION_ID","PAY_TYPE","BEFORE_AMT","TXN_AMT","TXN_NUM","REWARD_AMT","DEP_AMT","CARD_BAL","CARD_TXN_FLG","RSN_CODE","RSN_DATE","TRANS_STAT","ORIG_TICKET_ID","DEV_CODE","DEV_SEQ","SAM_ID","OPERATOR_ID","SALE_DEV_CODE","CITY_CODE","TAC","STL_SEQ_NO","RSV_BUF","LAST_UPD_TMS","ORDER_NO","LASTBEFOREIN_STATION_ID","REWARD_TYPE"
    val schemas = Seq("cardNo","date","time","x1","x2","x3","x4","cardType","x5","x6","x7","inOutCode","stationID","lastStationID","x8","x9","TXN_AMT","x10","REWARD_AMT","x11","x12","x13","x14","x15","x16","x17","x18","x19","x20","x21","x22","x23","x24","x25","RSV_BUF","x26","x27","x28","x29"
    )
    val __railICData = spark.read.format("csv")
      .option("header", "true")
      .load(railICPath).toDF(schemas:_*).selectExpr("cardNo","cardType","date","time","stationID","lastStationID","inOutCode","RSV_BUF","TXN_AMT","REWARD_AMT")

    val tempRailIC = __railICData.selectExpr("lpad(substr(cardNo,-16),16,0) as cardNo","lpad(cardType,2,0) as cardType", "toMsTime2(concat(date,time)) as time","stationID","lastStationID","inOutCode","TXN_AMT","REWARD_AMT")
    val railSite = preprocessRailSiteData(spark, railSitePath)

    val tempRailIC2 = tempRailIC.join(railSite, tempRailIC("stationID") === railSite("siteID").substr(13,4)
    ).selectExpr("inOutCode","cardNo","cardType", "time", "gainBusNo(cardNo) as getOnBusNo","stationID", "siteName", "longitude", "latitude","value(TXN_AMT,REWARD_AMT) as value1","TXN_AMT as value2")
    val railICData = tempRailIC2.rdd.groupBy(_.getAs("cardNo").toString.substring(8,16)).map {
      case (id, iter) => {
        val list=iter.toList.sortWith((x, y) => x.getAs("time").toString < y.getAs("time").toString)
        var i = 0
        val listLength = list.length
        var Relist: List[RailIC] = List()
        for (i <- 0 to listLength - 2) {
          val route1=list(i).getAs("stationID").toString.substring(0,2)
          val site1=list(i).getAs("stationID").toString.substring(2,4)
          val route2=list(i+1).getAs("stationID").toString.substring(0,2)
          val site2=list(i+1).getAs("stationID").toString.substring(2,4)
          val cardNo1=list(i).getAs("cardNo").toString
          val cardNo2=list(i+1).getAs("cardNo").toString
          var cardNo=""
          if(cardNo1>cardNo2)
            cardNo=cardNo1
          else
            cardNo=cardNo2
          if (list(i).getAs("inOutCode") == "21" && list(i + 1).getAs("inOutCode") == "22") {
            Relist = RailIC(cardNo, list(i).getAs("cardType").toString,list(i).getAs("time").toString,"00000000","00000000000000"+route1, "000000"+route1,"000000000000"+route1+site1, "0000"+site1, list(i).getAs("siteName").toString, "2", list(i).getAs("longitude").toString, list(i).getAs("latitude").toString,
              list(i + 1).getAs("time").toString, "00000000000000"+route2, "000000"+route2,"000000000000"+route2+site2, "0000"+site2, list(i+1).getAs("siteName").toString, list(i + 1).getAs("longitude").toString, list(i + 1).getAs("latitude").toString,"",list(i+1).getAs("value1").toString,list(i+1).getAs("value2").toString
            ) :: Relist
          }
        }
        Relist
      }
    }.flatMap(u => u).toDF()
    railICData
//      case (id, iter) => {
//        val tempList = iter.toList.sortWith((x, y) => x.getAs("getOnTime").toString < y.getAs("getOnTime").toString)
//        var i = 0
//        val listLength = tempList.length
//        var Relist: List[RailIC] = List()
//        for (i <- 0 to listLength - 2) {
//          if (tempList(i).getAs("TRANS_CODE") == "21" && tempList(i + 1).getAs("TRANS_CODE") == "22") {
//            Relist = RailIC(tempList(i).getAs("cardNo").toString, tempList(i).getAs("getOnTime").toString, tempList(i).getAs("getOnBusNo").toString,tempList(i).getAs("getOnRouteID").toString, tempList(i).getAs("getOnRoute").toString,tempList(i).getAs("getOnSiteID").toString, tempList(i).getAs("getOnSite").toString, tempList(i).getAs("getOnSiteName").toString, tempList(i).getAs("upOrDown").toString, tempList(i).getAs("getOnlongitude").toString, tempList(i).getAs("getOnlatitude").toString,
//              tempList(i + 1).getAs("getOnTime").toString, tempList(i + 1).getAs("getOnRouteID").toString,tempList(i + 1).getAs("getOnRoute").toString,tempList(i + 1).getAs("getOnSiteID").toString, tempList(i + 1).getAs("getOnSite").toString, tempList(i + 1).getAs("getOnSiteName").toString, tempList(i + 1).getAs("getOnlongitude").toString, tempList(i + 1).getAs("getOnlatitude").toString,tempList(i).getAs("ChangeType").toString,tempList(i).getAs("value1").toString,tempList(i).getAs("value2").toString
//            ) :: Relist
//          }
//        }
//        Relist
//      }
//    }.flatMap(u => u).toDF()
  }
  //预处理公交站点数据
  def preprocessBusSiteData(spark: SparkSession, path: String): DataFrame = {

    //上行转为 0 ，下行转为 1
    spark.udf.register("toUpOrDown", (s: String) => {
      if(s=="上行") {
        "0"
      }
      else {
        "1"
      }
    })
    //公交原始站点数据
    val _busSiteData = spark.read.format("csv")
      .option("header", "true")
      .load(path)
    //val schemas = Seq("routeID", "route", "routeName", "UpDown", "siteName", "type", "site", "latitude", "longitude")
    val schemas = Seq("routeID", "route", "routeName","siteID","siteCode","siteName", "UpDown", "site", "type","longitude","latitude","distance")

    val busSiteData = _busSiteData.toDF(schemas: _*).selectExpr("routeID","lpad(route,8,0) as route", "siteID","lpad(site,6,0) as site", "siteName", "toUpOrDown(UpDown) as UpDown", "longitude", "latitude")
    busSiteData
  }
  //预处理轨道站点数据
  def preprocessRailSiteData(spark: SparkSession, path: String): DataFrame = {
    //获取线路号
    spark.udf.register("gainRouteID", (s: String) => {
      //2018-06-29 06:40:54.080000000
      "00000000000000" + s.substring(0, 2)
    })
    //获取线路号
    spark.udf.register("gainRoute", (s: String) => {
      //2018-06-29 06:40:54.080000000
      "000000" + s.substring(0, 2)
    })
    //获取站点号
    spark.udf.register("gainSite", (s: String) => {
      //2018-06-29 06:40:54.080000000
      "0000" + s.substring(2, 4)
    })

    //上下行方向
    spark.udf.register("toUpDown", (s: String) => {
      "2"
    })

    val _railSiteData = spark.read.format("csv")
      .option("header", "false")
      .load(path)
    val railSiteData = _railSiteData.selectExpr("gainRouteID(_c1) as routeID","gainRoute(_c1) as route","lpad(_c1,16,'0') as siteID", "gainSite(_c1) as site", "_c3 as siteName", "toUpDown(_c1) as UpDown", "_c5 as longitude", "_c6 as latitude")
    railSiteData
  }
  // 合并公交站点和轨道站点数据
  def mergeBusAndRailSiteData(spark: SparkSession, busSiteData: DataFrame, railSiteData: DataFrame): DataFrame = {
    val mergeBusAndRailSiteData = busSiteData.union(railSiteData)
    mergeBusAndRailSiteData
  }
  //预处理公交的车辆数据
  def preprocessBusData(spark:SparkSession,busPath: String):DataFrame = {
    val _busData=spark.read.format("csv").option("header","true").load(busPath)
    val schemas = Seq("carNum","x1","route","routeID")
    val busData=_busData.toDF(schemas:_*).selectExpr("lpad(carNum,8,0) as carNum","lpad(route,8,0) as route")
    busData
  }
  //预处理GPS数据
  def dealGpsData(spark:SparkSession,gpsDataPath:String):DataFrame = {
    import spark.implicits._
    //格式化时间yyyyMMddHHmmss转毫秒数，注册为udf
    spark.udf.register("toMsTime", (s: String) => {
      //2018-06-29 06:40:54.080000000
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", new Locale("en")).parse(
        s
      ).getTime
    })
    val _gpsData = spark.read.format("csv")
      .option("header", "true")
      .load(gpsDataPath)
    val schemas = Seq("cardID", "carNum", "gpsTime", "x2", "x3", "x4", "siteNum")
    val tempGpsData = _gpsData.toDF(schemas: _*).filter("siteNum!='0'").selectExpr("lpad(carNum,8,0) as carNum", "toMsTime(gpsTime) as gpsTime", "lpad(siteNum,6,0) as siteNum")
    val gpsData = tempGpsData.rdd.groupBy(u => u.getAs("carNum").toString + u.getAs("siteNum").toString).map {
      case (id, iter) => {
        val list = iter.toList.sortWith((x, y) => x.getAs("gpsTime").toString < y.getAs("gpsTime").toString)
        var result = list(0)
        var temp = list(0)
        var resultList: List[GpsData] = List()
        val length = list.length
        for (i <- 0 to length - 1) {
          if (list(i).getAs("gpsTime").toString.toLong - temp.getAs("gpsTime").toString.toLong > 600000) {
            resultList = GpsData(result(0).toString, result(1).toString, result(2).toString) :: resultList
            result = list(i)
            temp = list(i)
          }
          else {
            temp = list(i)
          }
        }
        resultList = GpsData(result(0).toString, result(1).toString, result(2).toString) :: resultList
        resultList
      }
    }.flatMap(u => u).toDF().sort("carNum", "gpsTime")
    val newGPSData = gpsData.rdd.groupBy(u => u.getAs("carNum").toString).map {
      case (id, iter) => {
        val list = iter.toList.sortWith((x, y) => x.getAs("gpsTime").toString < y.getAs("gpsTime").toString)
        val last = list.last
        var count = 0
        val length = list.length
        var resultList: List[GpsData2] = List()
        resultList = GpsData2(list(0).getAs("carNum").toString, list(0).getAs("gpsTime").toString, list(0).getAs("siteNum").toString, count) :: resultList
        for (i <- 1 to length - 1) {
          if (list(i-1).getAs("siteNum").toString < list(i).getAs("siteNum").toString) {
            resultList = GpsData2(list(i).getAs("carNum").toString, list(i).getAs("gpsTime").toString, list(i).getAs("siteNum").toString, count) :: resultList
          }
          else {
            if(i<length-2 && list(i-1).getAs("siteNum").toString.toInt+1==list(i+1).getAs("siteNum").toString.toInt) {
              count = count
              resultList = GpsData2(list(i).getAs("carNum").toString, list(i).getAs("gpsTime").toString, list(i).getAs("siteNum").toString, count) :: resultList
            }
            else {
              count = count + 1
              resultList = GpsData2(list(i).getAs("carNum").toString, list(i).getAs("gpsTime").toString, list(i).getAs("siteNum").toString, count) :: resultList
            }
          }
        }
        resultList
      }
    }.flatMap(u => u).toDF()
    val newGPSData2=newGPSData.rdd.groupBy(u => u.getAs("carNum").toString+u.getAs("count").toString).map{
      case (id, iter) => {

        val list=iter.toList
        var resultList: List[GpsList] = List()

        var siteList=""
        for( i <- 0 to list.length-1) {

          siteList=siteList+list(i).getAs("gpsTime").toString+","+list(i).getAs("siteNum").toString+";"
        }
        for( i <- 0 to list.length-1) {
          resultList=GpsList(list(i).getAs("carNum").toString,list(i).getAs("gpsTime").toString,list(i).getAs("siteNum").toString,
            list(i).getAs("count").toString.toInt,siteList)::resultList
        }
        resultList
      } //        iter.toList
    }.flatMap(u=>u).toDF()
    newGPSData2
  }
  //建立临近站点辅助表
  def buildSiteNearSiteData(spark: SparkSession, routeSiteData: DataFrame,busSiteData:DataFrame): DataFrame = {
    import spark.implicits._
    val schemas = Seq("routeID","route", "siteID","site", "siteName", "UpDown", "longitude", "latitude","routeID2", "route2","siteID2", "site2", "siteName2", "UpDown2", "longitude2", "latitude2")
    spark.udf.register("calcDistance", (_lon1: String, _lat1: String, _lon2: String, _lat2: String) => {
      try {
        val lon1 = _lon1.toDouble
        val lat1 = _lat1.toDouble
        val lon2 = _lon2.toDouble
        val lat2 = _lat2.toDouble
        if (lat1 != 0 && lon1 != 0 && lat2 != 0 && lon2 != 0) {
          val R = 6378.137
          val radLat1 = lat1 * Math.PI / 180
          val radLat2 = lat2 * Math.PI / 180
          val a = radLat1 - radLat2
          val b = lon1 * Math.PI / 180 - lon2 * Math.PI / 180
          val s = 2 * Math.sin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
          BigDecimal.decimal(s * R).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
        } else {
          BigDecimal.decimal(100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
        }
      }
      catch {
        case exception: Exception => {
          BigDecimal.decimal(100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toString()
        }
      }
    })

    val siteJoinsite = routeSiteData.crossJoin(busSiteData).toDF(schemas: _*)
    val siteNearSite = siteJoinsite
      .selectExpr("routeID","route", "siteID","site", "siteName", "UpDown", "longitude", "latitude",
        "routeID2","route2","siteID2","site2","siteName2", "UpDown2", "longitude2", "latitude2", "calcDistance(longitude,latitude,longitude2,latitude2) as distance")
      .filter(($"distance" <= 1.0 and $"distance" =!= 0E-18) or $"siteName" === $"siteName2")
    siteNearSite
  }


  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .appName("OD_01_PreprocessData")
      .master("local")  //提交到集群需注释掉
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    //公交站点、轨道站点、站点临近站点、历史记录路径从resources中的config进行获取
    val xml=XML.load(this.getClass.getClassLoader.getResource("config.xml"))
    val busSitePath=(xml\"busSitePath").text                          //公交站点路径
    val railSitePath=(xml\"railSitePath").text                        //轨道站点路径
    val busPath=(xml\"busPath").text                        //轨道站点路径
    val siteNearSitePath=(xml\"siteNearSitePath").text                //站点临近站点数据路径
    val historyPath=(xml\"historyPath").text                          //历史记录路径

    //这里将路径写死，如果有需要可以改为从配置文件中读取，或者从main函数的参数args中进行获取
    val busICPath="file:///F:/ODData/initData/ICDATA/BusICData/2019-11-20.txt"
    val railICPath="file:///F:/ODData/initData/ICDATA/RailICData/2019-11-20"
    val gpsDataPath="file:///F:/ODData/initData/GPSDATA/2019-11-20"
    val resultPath="file:///F:/ODData/resultData/2019-11-20"
    val travelChainPath="file:///F:/ODData/tempData/travelChain/2019-11-20"

    val _busICData = preprocessBusICData(spark,busICPath).filter("getOnBusNo='00035520'")
    //_busICData.show(100)
    //val busICData=gainGetOnSite(spark,_busICData,gpsData)  //利用gps数据匹配公交IC的上车站点号，初始站点可靠的情况下可以省略这一步，直接使用PreprocessBusICData的结果作为busICData
    val busSiteData = preprocessBusSiteData(spark,busSitePath)
    val railSiteData = preprocessRailSiteData(spark,railSitePath)
    val siteNearSiteData=spark.read.format("csv").option("header","true").load(siteNearSitePath)

    val busData= preprocessBusData(spark,busPath)
    //busData.show(100)

    val gpsData=dealGpsData(spark,gpsDataPath)
    gpsData.write.mode(SaveMode.Overwrite).option("header","true").csv("file:///F:/ODData/tempData/GPSDATA/tempGpsData.csv")
//    val gpsData=spark.read.format("csv").option("header","true").load("file:///F:/ODData/tempData/GPSDATA/2019-11-20")
//    val newGPSData = gpsData.rdd.groupBy(u => u.getAs("carNum").toString).map {
//      case (id, iter) => {
//        val list = iter.toList.sortWith((x, y) => x.getAs("gpsTime").toString < y.getAs("gpsTime").toString)
//        val last = list.last
//        var count = 0
//        val length = list.length
//        var resultList: List[GpsData2] = List()
//        resultList = GpsData2(list(0).getAs("carNum").toString, list(0).getAs("gpsTime").toString, list(0).getAs("siteNum").toString, count) :: resultList
//        for (i <- 1 to length - 1) {
//          if (list(i-1).getAs("siteNum").toString < list(i).getAs("siteNum").toString) {
//            resultList = GpsData2(list(i).getAs("carNum").toString, list(i).getAs("gpsTime").toString, list(i).getAs("siteNum").toString, count) :: resultList
//          }
//          else {
//            if(i<length-2 && list(i-1).getAs("siteNum").toString.toInt+1==list(i+1).getAs("siteNum").toString.toInt) {
//              count = count
//              resultList = GpsData2(list(i).getAs("carNum").toString, list(i).getAs("gpsTime").toString, list(i).getAs("siteNum").toString, count) :: resultList
//            }
//            else {
//              count = count + 1
//              resultList = GpsData2(list(i).getAs("carNum").toString, list(i).getAs("gpsTime").toString, list(i).getAs("siteNum").toString, count) :: resultList
//            }
//          }
//        }
//        resultList
//      }
//    }.flatMap(u => u).toDF()
//    //newGPSData.show(10000)
//    println(newGPSData.count())
//    busData
//    val data=newGPSData.join(busData,newGPSData("carNum")===busData("carNum"),"left")
//    data.filter($"route".isNull).show(100)
//    println(newGPSData.count())
//    println(data.count())
    spark.stop()

    //val routeSiteData=mergeBusAndRailSiteData(spark,busSiteData,railSiteData)
    //val busSiteData=PreprocessBusSiteData(spark,busSitePath)
    //val busICData = PreprocessBusICData(spark,busICPath)
    //busICData.show(100)
    //val railICData = PreprocessRailICData(spark,railICPa c     th,railSitePath)
    //railICData.show()
    //println(railICData.count())
    //railICData.orderBy("cardNo","time").show(100)
//      .option("header", "true")
//      .load(railICPath).selectExpr("TICKET_ID","TXN_DATE","TXN_TIME","TICKET_TYPE","TXN_STATION_ID","LAST_STATION_ID","TRANS_CODE","RSV_BUF","TXN_AMT","REWARD_AMT")
//    val _railICData=__railICData.selectExpr("TICKET_ID","substr(TICKET_ID,-8) as cardNo","TXN_DATE","TXN_TIME","TICKET_TYPE","TXN_STATION_ID","LAST_STATION_ID","TRANS_CODE","RSV_BUF","TXN_AMT","REWARD_AMT")
//    val value = _railICData.filter("TICKET_TYPE=='0'")
//    value.show(100)
//    val count1=value.count()
//    val count2=value.filter("TRANS_CODE=='21'").count()
//    val count3=value.filter("TRANS_CODE=='22'").count()
//    println("21的数量"+count2)
//    println("22的数量"+count3)
//    println("总的数量"+count1)

//    val _busSiteData = spark.read.format("csv")
//      .option("header", "true")
//      .load(busSitePath)
//    //val schemas = Seq("routeID", "route", "routeName", "UpDown", "siteName", "type", "site", "latitude", "longitude")
//    val schemas = Seq("routeID", "route", "routeName","siteID","siteCode","siteName", "UpDown", "site", "type","longitude","latitude","distance")
//
//    val busSiteData = _busSiteData.toDF(schemas: _*)
//    //busSiteData.show(100)
//    //println(busSiteData.count())
//    val lineData=spark.read.format("csv").option("header","false").load("file:///C:/Users/Chenyang/Desktop/busLine.csv")
//    //lineData.show(100)
//    val m = busSiteData.join(lineData, busSiteData("routeID") === lineData("_c0")).select("routeID", "route", "routeName", "siteID", "siteCode", "siteName", "UpDown", "site", "type", "longitude", "latitude", "distance")
//    m.write.mode(SaveMode.Overwrite).option("header","true").csv("file:///F:/ODData/initData/SITEDATA/busSiteData1_4")
    //busSiteData.groupBy("route","site").count().filter("count>=2").select("route").distinct().show(1000)
    //busSiteData.filter("route=='00000118'").show(100)
    spark.stop()
  }
}
