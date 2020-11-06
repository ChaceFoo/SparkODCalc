package com.od

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import com.od.Helper._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OD_03_ChangeDataAndSingleODData {

  def getChangeType(spark:SparkSession,_odResult:DataFrame): DataFrame = {
    import spark.implicits._

    //格式化时间yyyyMMddHHmmss转毫秒数，注册为udf
    spark.udf.register("toMsTime", (s: String) => {
      //2018-06-29 06:40:54.080000000
      new SimpleDateFormat("yyyyMMddHHmmss", new Locale("en")).parse(
        s
      ).getTime
    })


    spark.udf.register("tranTimeToString",(s:String)=> {
      if(s(0)=='-') {
        "00000000000000"
      }
      else {
        val fm = new SimpleDateFormat("yyyyMMddHHmmss")
        val tim = fm.format(new Date(s.toLong))
        tim
      }
    })
    val odResult=_odResult.selectExpr("cardNo","getOnTime","getOnBusNo","getOnRouteID","getOnRoute","getOnSiteID","getOnSite","getOnSiteName","upOrDown","getOnLongitude","getOnLatitude",
      "getOffTime","getOffRouteID","getOffRoute","getOffSiteID","getOffSite","getOffSiteName","getOffLongitude","getOffLatitude","changeType","value1","value2")
//    val odResult=_odResult.selectExpr("cardNo","getOnTime","getOnBusNo","getOnRouteID","getOnRoute","getOnSiteID","getOnSite","getOnSiteName","upOrDown","getOnLongitude","getOnLatitude",
//      "getOffTime","getOffRouteID","getOffRoute","getOffSiteID","getOffSite","getOffSiteName","getOffLongitude","getOffLatitude","changeType","value1","value2")

    val tempResult = odResult.rdd.groupBy(u => u.getAs("cardNo").toString.substring(8,16))
      .map {
        case (id, iter) => {
          val list = iter.toList.sortWith((x, y) => x.getAs("getOnTime").toString < y.getAs("getOnTime").toString)
          val length = list.length
          var temp = list(0);
          var resultList: List[AllODResult] = List()
          resultList = AllODResult(temp.getAs("cardNo").toString, temp(1).toString, temp(2).toString, temp(3).toString, temp(4).toString,
            temp(5).toString, temp(6).toString, temp(7).toString, temp(8).toString, temp(9).toString, temp(10).toString,
            temp(11).toString, temp(12).toString, temp(13).toString, temp(14).toString, temp(15).toString,
            temp(16).toString, temp(17).toString, temp(18).toString,
            "0", temp(20).toString, temp(21).toString
          ) :: resultList
          for (i <- 1 to length - 1) {
            temp = list(i - 1);
            if (list(i).getAs("getOnTime").toString.toLong - temp.getAs("getOnTime").toString.toLong <= 3600000 && temp.getAs("getOnSiteID").toString != list(i).getAs("getOnSiteID").toString) {
              if (temp.getAs("upOrDown") == "1" || temp.getAs("upOrDown") == "0") {
                //公交换乘公交
                if (list(i).getAs("upOrDown") == "1" || list(i).getAs("upOrDown") == "0") {
                  resultList = AllODResult(String.valueOf(list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
                    String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                    String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)), "3", String.valueOf(list(i)(20)), String.valueOf(list(i)(21))) :: resultList
                }
                //公交换乘轨道
                else {
                  resultList = AllODResult(String.valueOf(list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
                    String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                    String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)), "1", String.valueOf(list(i)(20)), String.valueOf(list(i)(21))) :: resultList
                }
              }
              else {
                //轨道换乘公交
                if (list(i).getAs("upOrDown") == "1" || list(i).getAs("upOrDown") == "0") {
                  resultList = AllODResult(String.valueOf(list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
                    String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                    String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)), "2", String.valueOf(list(i)(20)), String.valueOf(list(i)(21))) :: resultList
                }
                //轨道换乘轨道
                else {
                  resultList = AllODResult(String.valueOf(list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
                    String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                    String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)), "4", String.valueOf(list(i)(20)), String.valueOf(list(i)(21))) :: resultList
                }
              }
            }
            else {
              resultList = AllODResult(String.valueOf(list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
                String.valueOf(list(i)(8)), String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)), "0", String.valueOf(list(i)(20)), String.valueOf(list(i)(21))) :: resultList
            }
          }
          resultList
        }
      }.flatMap(u => u).toDF()
    val result=tempResult.selectExpr("cardNo","tranTimeToString(getOnTime) as getOnTime","getOnBusNo","getOnRouteID","getOnRoute","getOnSiteID","getOnSite","getOnSiteName","upOrDown","getOnLongitude","getOnLatitude",
      "tranTimeToString(getOffTime) as getOffTime","getOffRouteID","getOffRoute","getOffSiteID","getOffSite","getOffSiteName","getOffLongitude","getOffLatitude","changeType","value1","value2")
    result
  }

  def getChangeData(spark:SparkSession,_odResult:DataFrame):DataFrame= {
      import spark.implicits._
      //格式化时间yyyyMMddHHmmss转毫秒数，注册为udf
      spark.udf.register("toMsTime", (s: String) => {
        //2018-06-29 06:40:54.080000000
        new SimpleDateFormat("yyyyMMddHHmmss", new Locale("en")).parse(
          s
        ).getTime
      })

      spark.udf.register("tranTimeToString", (s: String) => {
        if (s(0) == '-') {
          "00000000000000"
        }
        else {
          val fm = new SimpleDateFormat("yyyyMMddHHmmss")
          val tim = fm.format(new Date(s.toLong))
          tim
        }
      })

      val odResult = _odResult.selectExpr("cardNo", "toMsTime(getOnTime) as getOnTime", "getOnBusNo", "getOnRouteID", "getOnRoute", "getOnSiteID", "getOnSite", "getOnSiteName", "upOrDown", "getOnLongitude", "getOnLatitude",
        "toMsTime(getOffTime) as getOffTime", "getOffRouteID", "getOffRoute", "getOffSiteID", "getOffSite", "getOffSiteName", "getOffLongitude", "getOffLatitude", "changeType", "value1", "value2")
      val _changeDataResult = odResult.rdd.groupBy(u => u.getAs("cardNo").toString.substring(8, 16))
        .map {
          case (id, iter) => {
            val list = iter.toList.sortWith((x, y) => x.getAs("getOnTime").toString < y.getAs("getOnTime").toString)
            val length = list.length
            var temp = list(0);
            var resultList: List[ChangeData] = List()
            for (i <- 1 to length - 1) {
              temp = list(i - 1);
              if (list(i).getAs("getOnTime").toString.toLong - temp.getAs("getOnTime").toString.toLong <= 3600000 && temp.getAs("getOnSiteID").toString != list(i).getAs("getOnSiteID").toString) {
                if (temp.getAs("upOrDown") == "1" || temp.getAs("upOrDown") == "0") {
                  //公交换乘公交
                  if (list(i).getAs("upOrDown") == "1" || list(i).getAs("upOrDown") == "0") {
                    resultList = ChangeData(String.valueOf(temp(0)), String.valueOf(temp(1)), String.valueOf(temp(11)), String.valueOf(temp(2)), String.valueOf(temp(3)), String.valueOf(temp(12)), String.valueOf(temp(5)), String.valueOf(temp(14)),
                      String.valueOf(temp(8)), String.valueOf(temp(7)), String.valueOf(temp(16)),
                      String.valueOf(list(i)(1)), String.valueOf(list(i)(11)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(12)), String.valueOf(list(i)(5)), String.valueOf(list(i)(14)),
                      String.valueOf(list(i)(8)), String.valueOf(list(i)(7)), String.valueOf(list(i)(16))
                    ) :: resultList
                  }
                  //公交换乘轨道
                  else {
                    resultList = ChangeData(String.valueOf(list(i)(0)), String.valueOf(temp(1)), String.valueOf(temp(11)), String.valueOf(temp(2)), String.valueOf(temp(3)), String.valueOf(temp(12)), String.valueOf(temp(5)), String.valueOf(temp(14)),
                      String.valueOf(temp(8)), String.valueOf(temp(7)), String.valueOf(temp(16)),
                      String.valueOf(list(i)(1)), String.valueOf(list(i)(11)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(12)), String.valueOf(list(i)(5)), String.valueOf(list(i)(14)),
                      String.valueOf(list(i)(8)), String.valueOf(list(i)(7)), String.valueOf(list(i)(16))
                    ) :: resultList
                  }
                }
                else {
                  //轨道换乘公交
                  if (list(i).getAs("upOrDown") == "1" || list(i).getAs("upOrDown") == "0") {
                    resultList = ChangeData(String.valueOf(temp(0)), String.valueOf(temp(1)), String.valueOf(temp(11)), String.valueOf(temp(2)), String.valueOf(temp(3)), String.valueOf(temp(12)), String.valueOf(temp(5)), String.valueOf(temp(14)),
                      String.valueOf(temp(8)), String.valueOf(temp(7)), String.valueOf(temp(16)),
                      String.valueOf(list(i)(1)), String.valueOf(list(i)(11)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(12)), String.valueOf(list(i)(5)), String.valueOf(list(i)(14)),
                      String.valueOf(list(i)(8)), String.valueOf(list(i)(7)), String.valueOf(list(i)(16))
                    ) :: resultList
                  }
                  //轨道换乘轨道
                  else {
                    resultList = ChangeData(String.valueOf(temp(0)), String.valueOf(temp(1)), String.valueOf(temp(11)), String.valueOf(temp(2)), String.valueOf(temp(3)), String.valueOf(temp(12)), String.valueOf(temp(5)), String.valueOf(temp(14)),
                      String.valueOf(temp(8)), String.valueOf(temp(7)), String.valueOf(temp(16)),
                      String.valueOf(list(i)(1)), String.valueOf(list(i)(11)), String.valueOf(list(i)(2)), String.valueOf(list(i)(3)), String.valueOf(list(i)(12)), String.valueOf(list(i)(5)), String.valueOf(list(i)(14)),
                      String.valueOf(list(i)(8)), String.valueOf(list(i)(7)), String.valueOf(list(i)(16))
                    ) :: resultList
                  }
                }
              }
            }
            resultList
          }
        }.flatMap(u => u).toDF()
      val changeDataResult = _changeDataResult.selectExpr("cardNo", "tranTimeToString(preOnTime) as preOnTime", "tranTimeToString(preOffTime) as preOffTime", "preBusNo", "preOnRouteID", "preOffRouteID", "preOnSiteID", "preOffSiteID", "preUpOrDown", "preOnSiteName", "preOffSiteName",
        "tranTimeToString(currOnTime) as currOnTime", "tranTimeToString(currOffTime) as currOffTime", "currBusNo", "currOnRouteID", "currOffRouteID", "currOnSiteID", "currOffSiteID", "currUpOrDown", "currOnSiteName", "currOffSiteName").filter("currOnTime>preOffTime")
      changeDataResult
    }
  def getSingleODData(spark:SparkSession,_odResult:DataFrame):DataFrame={

    import spark.implicits._

    //格式化时间yyyyMMddHHmmss转毫秒数，注册为udf
    spark.udf.register("toMsTime", (s: String) => {
      //2018-06-29 06:40:54.080000000
      new SimpleDateFormat("yyyyMMddHHmmss", new Locale("en")).parse(
        s
      ).getTime
    })

    spark.udf.register("tranTimeToString",(s:String)=> {
      if(s(0)=='-') {
        "00000000000000"
      }
      else {
        val fm = new SimpleDateFormat("yyyyMMddHHmmss")
        val tim = fm.format(new Date(s.toLong))
        tim
      }
    })

    val odResult=_odResult.selectExpr("cardNo","toMsTime(getOnTime) as getOnTime","getOnBusNo","getOnRouteID","getOnRoute","getOnSiteID","getOnSite","getOnSiteName","upOrDown","getOnLongitude","getOnLatitude",
      "toMsTime(getOffTime) as getOffTime","getOffRouteID","getOffRoute","getOffSiteID","getOffSite","getOffSiteName","getOffLongitude","getOffLatitude","changeType","value1","value2")
    val _singleODResult=odResult.rdd.groupBy(u=>u.getAs("cardNo").toString.substring(8,16))
      .map {
        case (id, iter) => {
          val list = iter.toList.sortWith((x, y) => x.getAs("getOnTime").toString < y.getAs("getOnTime").toString)
          val length = list.length
          var temp = list(0);
          var resultList: List[SingleOD] = List()
          resultList = SingleOD(String.valueOf(temp(0)), String.valueOf(temp(1)), String.valueOf(temp(3)), String.valueOf(temp(4)), String.valueOf(temp(5)), String.valueOf(temp(6)), String.valueOf(temp(7)),
            String.valueOf(temp(9)), String.valueOf(temp(10)), String.valueOf(temp(11)), String.valueOf(temp(12)), String.valueOf(temp(13)), String.valueOf(temp(14)), String.valueOf(temp(15)),
            String.valueOf(temp(16)), String.valueOf(temp(17)), String.valueOf(temp(18)),String.valueOf(temp(20)), String.valueOf(temp(21))) :: resultList
          temp = list(0);
          for (i <- 1 to length - 1) {
            if (list(i).getAs("getOnTime").toString.toLong - temp.getAs("getOnTime").toString.toLong <= 3600000 && temp.getAs("getOnSiteID").toString != list(i).getAs("getOnSiteID").toString) {
              resultList=resultList.drop(1)
              resultList = SingleOD(String.valueOf(temp(0)), String.valueOf(temp(1)), String.valueOf(temp(3)), String.valueOf(temp(4)), String.valueOf(temp(5)), String.valueOf(temp(6)), String.valueOf(temp(7)),
                String.valueOf(temp(9)), String.valueOf(temp(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)),(String.valueOf(list(i)(20)).toInt+String.valueOf(temp(20)).toInt).toString, (String.valueOf(list(i)(21)).toInt+String.valueOf(temp(21)).toInt).toString)::resultList
            }
            else{
              resultList = SingleOD(String.valueOf(list(i)(0)), String.valueOf(list(i)(1)), String.valueOf(list(i)(3)), String.valueOf(list(i)(4)), String.valueOf(list(i)(5)), String.valueOf(list(i)(6)), String.valueOf(list(i)(7)),
                String.valueOf(list(i)(9)), String.valueOf(list(i)(10)), String.valueOf(list(i)(11)), String.valueOf(list(i)(12)), String.valueOf(list(i)(13)), String.valueOf(list(i)(14)), String.valueOf(list(i)(15)),
                String.valueOf(list(i)(16)), String.valueOf(list(i)(17)), String.valueOf(list(i)(18)),String.valueOf(list(i)(20)), String.valueOf(list(i)(21)))::resultList
              temp=list(i)
            }
          }
          resultList
        }
      }.flatMap(u => u).toDF()
    val singleODResult=_singleODResult.selectExpr("cardNo","tranTimeToString(getOnTime) as getOnTime","getOnRouteID","getOnRoute","getOnSiteID", "getOnSite","getOnSiteName","getOnLongitude","getOnLatitude",
      "tranTimeToString(getOffTime) as getOffTime","getOffRouteID","getOffRoute","getOffSiteID", "getOffSite","getOffSiteName","getOffLongitude","getOffLatitude","value1","value2")
    singleODResult
  }
}
