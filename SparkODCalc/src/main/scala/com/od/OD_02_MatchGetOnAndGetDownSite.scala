package com.od

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import com.od.Helper._
import com.od.OD_01_PreprocessData._
import com.od.OD_02_MatchGetOnAndGetDownSite.matchGetOffSiteWithNullData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

import scala.util.control.Breaks
import scala.xml.XML

object OD_02_MatchGetOnAndGetDownSite {
  def gainGetOnSite(spark:SparkSession,_busICData:DataFrame,gpsData:DataFrame): DataFrame = {
    import spark.implicits._
    //时间差
    spark.udf.register("timeDifference",(time1:String,time2:String)=> {
      time1.toLong-time2.toLong
    })
    val gainGetOnSite = _busICData.join(gpsData, _busICData("getOnBusNo") === gpsData("carNum") and
      _busICData("getOnTime") > gpsData("gpsTime")).selectExpr("cardNo","cardType", "getOnTime", "getOnBusNo", "getOnRoute", "getOnSite", "siteNum as getOnSite2", "changeType", "value1", "value2", "timeDifference(getOnTime,gpsTime) as difference","count","siteList")
      .rdd.groupBy(u => u.getAs("cardNo").toString + u.getAs("getOnTime"))
      .map {
        case (id, iter) => {
          iter.toList.sortWith((x, y) => x.getAs("difference").toString.toLong > y.getAs("difference").toString.toLong).last
        }
        //  case class BusIC(cardNo: String,getOnTime: String,getOnBusNo: String,getOnRoute: String,getOnSite: String,getOnSite2: String,difference:String)
      }.map(u => BusIC(u.getAs("cardNo").toString, u.getAs("cardType").toString,u.getAs("getOnTime").toString, u.getAs("getOnBusNo").toString,
      u.getAs("getOnRoute").toString, u.getAs("getOnSite").toString, u.getAs("getOnSite2").toString,u.getAs("changeType").toString,u.getAs("value1").toString,u.getAs("value2").toString, u.getAs("difference").toString,u.getAs("count").toString,u.getAs("siteList").toString)
    ).toDF()
    val busICData = gainGetOnSite.selectExpr("cardNo", "cardType","getOnTime", "getOnBusNo", "getOnRoute", "getOnSite2 as getOnSite", "changeType", "value1", "value2","count","siteList")
    busICData
  }

  def matchGetOnSite(spark:SparkSession,busICData:DataFrame,busSiteData:DataFrame):DataFrame={
    val matchGetOnSiteData = busICData.join(busSiteData,
      busICData("getOnRoute") === busSiteData("route") and
        busICData("getOnSite") === busSiteData("site"),"left"
    ).selectExpr("cardNo","cardType", "getOnTime", "getOnBusNo", "routeID as getOnRouteID","getOnRoute","siteID as getOnSiteID", "getOnSite",
      "siteName as getOnSiteName", "UpDown as upOrDown",
      "longitude as getOnLongitude", "latitude as getOnLatitude", "changeType","value1","value2","count","siteList")
    matchGetOnSiteData
  }

  //包含轨道IC数据的上车站点
  def matchGetOnSiteWithRailIC(spark:SparkSession,matchGetOnSiteData:DataFrame,railICData:DataFrame):DataFrame={
    val railICWithGetDownData=railICData.selectExpr("cardNo","cardType", "getOnTime","getOnBusNo", "getOnRouteID","getOnRoute","getOnSiteID", "getOnSite","getOnSiteName","upOrDown", "getOnlongitude", "getOnlatitude","changeType","value1","value2","value1 as count","value2 as siteList")
    val matchGetOnSiteWithRailICData = matchGetOnSiteData.union(railICWithGetDownData)
    matchGetOnSiteWithRailICData
  }

  //出行链
  def travelChain(spark:SparkSession,matchGetOnSiteWithRailICData:DataFrame): DataFrame = {
    import spark.implicits._
    val travelChainData = matchGetOnSiteWithRailICData.rdd.groupBy(u =>u.getAs("cardNo").toString.substring(8,16)) //根据ID进行分组
      .map { case (id, iter) => {
        val list = iter.toList.sortWith((x, y) => x.getAs("getOnTime").toString < y.getAs("getOnTime").toString)
        val i = 0
        val lLength = list.length
        var Relist: List[NewIC] = List()
        for (i <- 0 to lLength - 1) {

          if (i != lLength - 1) {
            Relist = NewIC(String.valueOf(
              list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)),
              String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
              String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)), String.valueOf(list(i)(16)), String.valueOf(list((i + 1) % lLength)(5)), String.valueOf(list((i + 1) % lLength)(7)), String.valueOf(list((i + 1) % lLength)(2))) :: Relist
          }
          else {
            Relist = NewIC(String.valueOf(
              list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)),
              String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
              String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)), String.valueOf(list(i)(16)), String.valueOf(list((i + 1) % lLength)(5)), String.valueOf(list((i + 1) % lLength)(7)), "5000000000000") :: Relist
          }
        }
        Relist
      }
      }.flatMap(u => u).toDF().filter("upOrDown!='2'")
    travelChainData
  }
  def matchGetOffSiteWithNullData(spark:SparkSession,travelChainData:DataFrame,siteNearSiteData:DataFrame): DataFrame = {
    import spark.implicits._
    //字符串表示的距离转浮点数
    spark.udf.register("toFloat",(s:String)=> {
      if(s ==null) 0.0f
      else s.toFloat
    })
    val tempMatchGetDownSiteData = travelChainData.join(siteNearSiteData,
      travelChainData("targetRoute") === siteNearSiteData("route")
        and travelChainData("targetSite") >= siteNearSiteData("site")
        and travelChainData("getOnRoute") === siteNearSiteData("route2")
        and travelChainData("upOrDown") === siteNearSiteData("UpDown2")
        and travelChainData("getOnSite") < siteNearSiteData("site2"), "left")
      .selectExpr("cardNo", "cardType","getOnTime", "getOnBusNo", "getOnRouteID","getOnRoute", "getOnSiteID","getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude", "targetRoute", "targetSite", "routeID2","route2", "siteID2","site2", "siteName2", "longitude2", "latitude2", "toFloat(distance) as distance","changeType","value1","value2","count","siteList","nextTime")
//    val matchGetDownSiteData=tempMatchGetDownSiteData.rdd.groupBy(u => u.getAs("cardNo").toString.substring(8,16)+u.getAs("getOnTime"))
//      .map {
//        case (id, iter) =>
//          iter.toList
//            .sortWith((x, y) => x.getAs("distance").toString > y.getAs("distance").toString).last
//      }.map(u => MatchGetOffSite(String.valueOf(u(0)), String.valueOf(u(1)), String.valueOf(u(2)), String.valueOf(u(3)), String.valueOf(u(4)), String.valueOf(u(5)),
//      String.valueOf(u(6)), String.valueOf(u(7)), String.valueOf(u(8)), String.valueOf(u(9)),String.valueOf(u(10)),
//      String.valueOf(u(13)), String.valueOf(u(14)), String.valueOf(u(15)), String.valueOf(u(16)), String.valueOf(u(17)), String.valueOf(u(18)),String.valueOf(u(19)),
//      String.valueOf(u(21)),String.valueOf(u(22)),String.valueOf(u(23))
//    )).toDF
//    val matchGetOffSiteWithNullData=tempMatchGetDownSiteData.rdd.groupBy(u => u.getAs("cardNo").toString.substring(8,16)+u.getAs("getOnTime"))
//      .map{
//        //  case class TempResult(cardNo: String,cardType:String,getOnTime: String,getOnBusNo: String,getOnRouteID: String,getOnRoute: String,getOnSiteID: String,getOnSite: String,getOnSiteName: String,upOrDown: String,getOnLongitude: String,getOnLatitude: String,
//        //                   changeType:String,value1:String,value2:String,count:String,siteList:String,getOffRoute: String,getOffSite: String,getOffTime:String,nextTime:String)
//        case(id,iter)=>{
//          val list=iter.toList
//          val length=list.length
//          val siteList=list(0).getAs("siteList").toString.split(";")
//          val length2=siteList.length
//          var resultlist:List[TempResult]=List()
//          val inner = new Breaks;
//          for(i <- 0 to length-1) {
//            inner.breakable {
//              for (j <- 0 to length2-1) {
//                if (siteList(j).toString.substring(0, 13) < list(i).getAs("nextTime").toString &&
//                  siteList(j).toString.substring(14, 20) <= list(i).getAs("site2").toString) {
//                  resultlist = TempResult(list(i).getAs("cardNo").toString, list(i).getAs("cardType").toString,
//                    list(i).getAs("getOnTime").toString, list(i).getAs("getOnBusNo").toString, list(i).getAs("getOnRouteID").toString,
//                    list(i).getAs("getOnRoute").toString, list(i).getAs("getOnSiteID").toString, list(i).getAs("getOnSite").toString,
//                    list(i).getAs("getOnSiteName").toString, list(i).getAs("upOrDown").toString, list(i).getAs("getOnLongitude").toString,
//                    list(i).getAs("getOnLatitude").toString, list(i).getAs("changeType").toString, list(i).getAs("value1").toString,
//                    list(i).getAs("value2").toString, list(i).getAs("count").toString, list(i).getAs("siteList").toString, list(i).getAs("getOnRoute").toString,
//                    siteList(j).toString.substring(14, 20), siteList(j).toString.substring(0, 13), list(i).getAs("nextTime").toString, list(i).getAs("distance").toString
//                  ) :: resultlist
//                  inner.break
//                }
//              }
//            }
//          }
//          resultlist.sortWith((x,y) => x.distance>y.distance).last
//        }
//      }.toDF()
    //matchGetOffSiteWithNullData
    tempMatchGetDownSiteData
  }

  //匹配成功的数据
  //匹配成功的数据
  def matchGetDownSiteDataSuccess(spark:SparkSession,matchGetDownSiteData:DataFrame): DataFrame = {
    import spark.implicits._
    val matchGetDownSiteDataSuccess=matchGetDownSiteData.filter($"siteID2".isNotNull).filter("getOnSiteID<=siteID2")
    val matchGetDownSiteDataSuccessData=matchGetDownSiteDataSuccess.rdd.groupBy(u => u.getAs("cardNo").toString.substring(8,16)+u.getAs("getOnTime"))
      .map{
        //  case class TempResult(cardNo: String,cardType:String,getOnTime: String,getOnBusNo: String,getOnRouteID: String,getOnRoute: String,getOnSiteID: String,getOnSite: String,getOnSiteName: String,upOrDown: String,getOnLongitude: String,getOnLatitude: String,
        //                   changeType:String,value1:String,value2:String,count:String,siteList:String,getOffRoute: String,getOffSite: String,getOffTime:String,nextTime:String)
        case(id,iter)=>{
          val list=iter.toList
          val length=list.length
          val siteList=list(0).getAs("siteList").toString.split(";")
          val length2=siteList.length
          var resultlist:List[TempResult]=List()
          val inner = new Breaks;
          for(i <- 0 to length-1) {
            inner.breakable {
              for (j <- 0 to length2-1) {
                if (siteList(j).toString.substring(0, 13) < list(i).getAs("nextTime").toString &&
                  siteList(j).toString.substring(14, 20) <= list(i).getAs("site2").toString) {
                  resultlist = TempResult(list(i).getAs("cardNo").toString, list(i).getAs("cardType").toString,
                    list(i).getAs("getOnTime").toString, list(i).getAs("getOnBusNo").toString, list(i).getAs("getOnRouteID").toString,
                    list(i).getAs("getOnRoute").toString, list(i).getAs("getOnSiteID").toString, list(i).getAs("getOnSite").toString,
                    list(i).getAs("getOnSiteName").toString, list(i).getAs("upOrDown").toString, list(i).getAs("getOnLongitude").toString,
                    list(i).getAs("getOnLatitude").toString, list(i).getAs("changeType").toString, list(i).getAs("value1").toString,
                    list(i).getAs("value2").toString, list(i).getAs("count").toString, list(i).getAs("siteList").toString, list(i).getAs("getOnRoute").toString,
                    siteList(j).toString.substring(14, 20), siteList(j).toString.substring(0, 13), list(i).getAs("nextTime").toString, list(i).getAs("distance").toString
                  ) :: resultlist
                  inner.break
                }
              }
            }
          }
          resultlist.sortWith((x,y) => x.distance>y.distance).last
        }
      }.toDF()
    matchGetDownSiteDataSuccessData
  }

  //新建历史数据
  def newHistroyData(spark:SparkSession,matchGetDownSiteDataSuccessData:DataFrame,histroyDataPath:String): DataFrame= {
    //毫秒数转格式化时间yyyyMM
    spark.udf.register("tranTimeToData",(s:String)=> {
      if(s(0)=='-') {
        "000000"
      }
      else {
        val fm = new SimpleDateFormat("yyyyMM")
        val tim = fm.format(new Date(s.toLong))
        tim
      }
    })
    //新增列
    spark.udf.register("addCol",(s:String)=> {
      1
    })
    //count String-> Int
    spark.udf.register("toInt",(s:String)=> {
      s.toInt
    })
    val _newData = matchGetDownSiteDataSuccessData.selectExpr(
      "getOffRoute","getOffSite", "addCol(cardNo) as weight")
    try {
      val oldHistoryData=spark.read.format("csv").option("header","true").load(histroyDataPath).selectExpr("time","cardNo","getOnRouteID","getOnRoute","getOnSiteID","getOnSite","getOffRouteID","getOffRoute","getOffSiteID","getOffSite","getOffSiteName","getOffLongitude","getOffLatitude","toInt(count) as count")
      val newData=_newData.union(oldHistoryData)
      val newHistoryData=newData.groupBy("getOffRoute","getOffSite")
        .sum("weight").withColumnRenamed("sum(weight)","weight")
      newHistoryData
    }
    catch {
      case e:AnalysisException => {
        val newHistoryData=_newData.groupBy("getOffRoute","getOffSite")
          .sum("weight").withColumnRenamed("sum(weight)","weight")
        newHistoryData
      }
    }
  }
  //建立历史记录或更新历史记录
  def buidHistroyData(spark: SparkSession,newHistoryData:DataFrame,histroyDataPath:String): Unit= {
    //生成新的临时记录
    newHistoryData.write.mode(SaveMode.Overwrite).option("header","true").csv(histroyDataPath+"temp")
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    //删除原纪录
    val path = new Path(histroyDataPath)
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(path)){
      hdfs.delete(path,true)
    }
    val configuration = new Configuration
    val fs = FileSystem.get(new URI(""), configuration)
    //重命名文件
    val src = new Path(histroyDataPath+"temp")
    val dst = new Path(histroyDataPath)
    fs.rename(src,dst)
  }
  def matchGetDownSiteDataFail(spark:SparkSession,matchGetDownSiteData:DataFrame,weightData:DataFrame): DataFrame = {
    import spark.implicits._
    val matchGetDownSiteDataFail=matchGetDownSiteData.filter($"siteID2".isNull or $"getOnSiteID"===$"siteID2").selectExpr("cardNo","cardType", "getOnTime", "getOnBusNo", "getOnRouteID","getOnRoute","getOnSiteID", "getOnSite",
      "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude", "changeType","value1","value2","count","siteList","nextTime")
    val matchSuccessDataWithWeight = matchGetDownSiteDataFail.join(weightData,
      matchGetDownSiteDataFail("getOnRoute") === weightData("getOffRoute") and
        matchGetDownSiteDataFail("getOnSite") < weightData("getOffSite"))
      .selectExpr("cardNo", "cardType","getOnTime", "getOnBusNo","getOnRouteID", "getOnRoute", "getOnSiteID", "getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude", "getOffRoute", "getOffSite","weight","changeType","value1","value2","siteList","count","nextTime")
    val matchGetDownSiteDataFailData=matchSuccessDataWithWeight.rdd.groupBy(u => u.getAs("cardNo").toString.substring(8,16)+u.getAs("getOnTime"))
      .map{
        //  case class TempResult(cardNo: String,cardType:String,getOnTime: String,getOnBusNo: String,getOnRouteID: String,getOnRoute: String,getOnSiteID: String,getOnSite: String,getOnSiteName: String,upOrDown: String,getOnLongitude: String,getOnLatitude: String,
        //                   changeType:String,value1:String,value2:String,count:String,siteList:String,getOffRoute: String,getOffSite: String,getOffTime:String,nextTime:String)
        case(id,iter)=>{
          val list=iter.toList
          val length=list.length
          val siteList=list(0).getAs("siteList").toString.split(";")
          val length2=siteList.length
          var resultlist:List[TempResult]=List()
          val inner = new Breaks;

          for(i <- 0 to length-1) {
            inner.breakable {
              for (j <- 0 to length2-1) {
                if (siteList(j).toString.substring(0, 13) < list(i).getAs("nextTime").toString &&
                  siteList(j).toString.substring(14, 20) == list(i).getAs("getOffSite").toString) {
                  resultlist = TempResult(list(i).getAs("cardNo").toString, list(i).getAs("cardType").toString,
                    list(i).getAs("getOnTime").toString, list(i).getAs("getOnBusNo").toString, list(i).getAs("getOnRouteID").toString,
                    list(i).getAs("getOnRoute").toString, list(i).getAs("getOnSiteID").toString, list(i).getAs("getOnSite").toString,
                    list(i).getAs("getOnSiteName").toString, list(i).getAs("upOrDown").toString, list(i).getAs("getOnLongitude").toString,
                    list(i).getAs("getOnLatitude").toString, list(i).getAs("changeType").toString, list(i).getAs("value1").toString,
                    list(i).getAs("value2").toString, list(i).getAs("count").toString, list(i).getAs("siteList").toString, list(i).getAs("getOnRoute").toString,
                    siteList(j).toString.substring(14, 20), siteList(j).toString.substring(0, 13), list(i).getAs("nextTime").toString, list(i).getAs("weight").toString
                  ) :: resultlist
                  inner.break
                }
              }
            }
          }
          if(resultlist.length!=0) {
            var sum = 0
            var result = resultlist(0)
            for (i <- 0 to resultlist.length - 1) {
              sum = sum + resultlist(i).distance.toInt
            }
            var randNum = scala.util.Random.nextInt(sum)
            for (i <- 0 to resultlist.length - 1) {
              val value = resultlist(i).distance.toString.toInt
              if (value < randNum && value >= 0) {
                result = resultlist(i)
              }
            }
            result
          }
          else {
            val inner2 = new Breaks;
            var result2=TempResult(list(0).getAs("cardNo").toString, list(0).getAs("cardType").toString,
              list(0).getAs("getOnTime").toString, list(0).getAs("getOnBusNo").toString, list(0).getAs("getOnRouteID").toString,
              list(0).getAs("getOnRoute").toString, list(0).getAs("getOnSiteID").toString, list(0).getAs("getOnSite").toString,
              list(0).getAs("getOnSiteName").toString, list(0).getAs("upOrDown").toString, list(0).getAs("getOnLongitude").toString,
              list(0).getAs("getOnLatitude").toString, list(0).getAs("changeType").toString, list(0).getAs("value1").toString,
              list(0).getAs("value2").toString, list(0).getAs("count").toString, list(0).getAs("siteList").toString, list(0).getAs("getOnRoute").toString,
              "000000","2222222222222", list(0).getAs("nextTime").toString, list(0).getAs("weight").toString)
            inner2.breakable {
              for (j <- 0 to length2-1) {
                if (siteList(j).toString.substring(0, 13) < list(0).getAs("nextTime").toString &&
                  siteList(j).toString.substring(14, 20) > list(0).getAs("getOnSite").toString) {
                  result2=TempResult(list(0).getAs("cardNo").toString, list(0).getAs("cardType").toString,
                    list(0).getAs("getOnTime").toString, list(0).getAs("getOnBusNo").toString, list(0).getAs("getOnRouteID").toString,
                    list(0).getAs("getOnRoute").toString, list(0).getAs("getOnSiteID").toString, list(0).getAs("getOnSite").toString,
                    list(0).getAs("getOnSiteName").toString, list(0).getAs("upOrDown").toString, list(0).getAs("getOnLongitude").toString,
                    list(0).getAs("getOnLatitude").toString, list(0).getAs("changeType").toString, list(0).getAs("value1").toString,
                    list(0).getAs("value2").toString, list(0).getAs("count").toString, list(0).getAs("siteList").toString, list(0).getAs("getOnRoute").toString,
                    siteList(j).toString.substring(14,20),siteList(j).toString.substring(0,13), list(0).getAs("nextTime").toString, list(0).getAs("weight").toString)
                  inner2.break
                }
              }
            }
            result2
          }
        }
      }.toDF()
    matchGetDownSiteDataFailData
  }
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("OD_02_MatchGetOnAndGetDownSite")
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
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    //公交站点、轨道站点、站点临近站点、历史记录路径从resources中的config进行获取
    val xml=XML.load(this.getClass.getClassLoader.getResource("config.xml"))
    val busSitePath=(xml\"busSitePath").text                          //公交站点路径
    val railSitePath=(xml\"railSitePath").text                        //轨道站点路径
    val busPath=(xml\"busPath").text                        //轨道站点路径
    val siteNearSitePath=(xml\"siteNearSitePath").text                //站点临近站点数据路径
    val historyPath=(xml\"historyPath").text                          //历史记录路径

    //这里将路径写死，如果有需要可以改为从配置文件中读取，或者从main函数的参数args中进行获取
    val busICPath="hdfs://10.10.102.8:9000/ODData/initData/BusICData/2019-11-20.txt"
    val railICPath="hdfs://10.10.102.8:9000/ODData/initData/RailICData/2019-11-20"
    val gpsDataPath="hdfs://10.10.102.8:9000/ODData/initData/gpsData/2019-11-20"
    val ODResultPath="hdfs://10.10.102.8:9000/ODData/resultData/ODResult/2019-11-20"
    val travelChainPath="hdfs://10.10.102.8:9000/ODData/tempData/travelChain/2019-11-20"

    val _busICData = preprocessBusICData(spark,busICPath)
    val gpsData=dealGpsData(spark,gpsDataPath)
    val busICData=gainGetOnSite(spark,_busICData,gpsData)  //利用gps数据匹配公交IC的上车站点号，初始站点可靠的情况下可以省略这一步，直接使用PreprocessBusICData的结果作为busICData
    val busSiteData = preprocessBusSiteData(spark,busSitePath)
    val railSiteData = preprocessRailSiteData(spark,railSitePath)
    val siteNearSiteData=spark.read.format("csv").option("header","true").load(siteNearSitePath)
    //val matchGetOnSiteData=spark.read.format("csv").option("header","true").load("file:///F:/ODData/tempData/matchGetOnSiteData/2019-11-20")
    val matchGetOnSiteData=matchGetOnSite(spark,busICData,busSiteData)
    val railICData=preprocessRailICData(spark,railICPath,railSitePath)
    val matchGetOnSiteWithRailICData=matchGetOnSiteWithRailIC(spark,matchGetOnSiteData,railICData)
    val travelChainData=travelChain(spark,matchGetOnSiteWithRailICData)
    val matchGetOffSiteWithNull=matchGetOffSiteWithNullData(spark,travelChainData,siteNearSiteData)
    val matchGetDownSiteDataSuccessData=matchGetDownSiteDataSuccess(spark,matchGetOffSiteWithNull)
    val weightData=newHistroyData(spark,matchGetDownSiteDataSuccessData,historyPath)
    val matchGetDownSiteDataFailData=matchGetDownSiteDataFail(spark,matchGetOffSiteWithNull,weightData)
    val allData=matchGetDownSiteDataSuccessData.union(matchGetDownSiteDataFailData)
    val allData2=allData.selectExpr("cardNo", "cardType","getOnTime", "getOnBusNo","getOnRouteID", "getOnRoute", "getOnSiteID", "getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude", "getOffRoute", "getOffSite","changeType","value1","value2","getOffTime","nextTime")
        //.write.mode(SaveMode.Overwrite).option("header","true").csv("file:///F:/ODData/tempData/allData/2019-11-20")
    val result=allData2.join(busSiteData,
      allData2("getOffRoute") === busSiteData("route") and
        allData2("getOffSite") === busSiteData("site"),"left")
        .selectExpr("cardNo", "cardType","getOnTime", "getOnBusNo","getOnRouteID", "getOnRoute", "getOnSiteID", "getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude",
          "getOffTime", "routeID as getOffRouteID", "getOffRoute", "siteID as getOffSiteID", "getOffSite", "siteName as getOffSiteName", "longitude as getOffLongitude", "latitude as getOffLatitude","changeType","value1","value2")
    val ODResult=result.union(railICData)
    ODResult.write.mode(SaveMode.Overwrite).option("header","true").csv(ODResultPath)
    //println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
    spark.stop()
  }


  def main2(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("OD_02_MatchGetOnAndGetDownSite")
      .master("local[*]")  //提交到集群需注释掉
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //spark.sql.autoBroadcastJoinThreshold = -1
    //不限定小表的大小
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    // 每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设定的值
    spark.conf.set("spark.sql.join.preferSortMergeJoin", true)
    spark.conf.set("spark.debug.maxToStringFields", 5000)
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

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

    val _busICData = preprocessBusICData(spark,busICPath)
    val gpsData=dealGpsData(spark,gpsDataPath)
    //val gpsData=spark.read.option("header","true").format("csv").load("file:///F:/ODData/tempData/GPSDATA/tempGpsData.csv")
    val busICData=gainGetOnSite(spark,_busICData,gpsData)  //利用gps数据匹配公交IC的上车站点号，初始站点可靠的情况下可以省略这一步，直接使用PreprocessBusICData的结果作为busICData
    val busSiteData = preprocessBusSiteData(spark,busSitePath)
    val railSiteData = preprocessRailSiteData(spark,railSitePath)
    val siteNearSiteData=spark.read.format("csv").option("header","true").load(siteNearSitePath)
    val matchGetOnSiteData=matchGetOnSite(spark,busICData,busSiteData)
    matchGetOnSiteData.write.mode(SaveMode.Overwrite).option("header","true").csv("file:///F:/ODData/tempData/matchGetOnSiteData/2019-11-20")
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
    spark.stop()
  }
}
