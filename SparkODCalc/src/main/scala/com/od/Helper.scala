package com.od

object Helper {
  case class Rail(TICKET_ID:String,TXN_DATE:String,TXN_TIME:String,TXN_STATION_ID:String,TRANS_CODE:String,changeType:String,TXN_AMT:String,REWARD_AMT:String)
  case class RailIC(cardNo: String,cardType: String, getOnTime: String, getOnBusNo: String, getOnRouteID: String,getOnRoute: String,getOnSiteID:String, getOnSite: String, getOnSiteName: String, upOrDown: String, getOnLongitude: String, getOnLatitude: String,
                    getOffTime: String, getOffRouteID: String,getOffRoute: String,getOffSiteID:String, getOffSite: String, getOffSiteName: String, getOffLongitude: String, getOffLatitude: String,changeType:String,value1:String,value2:String)
  case class BusIC(cardNo: String,cardType:String,getOnTime: String,getOnBusNo: String,getOnRoute: String,getOnSite: String,getOnSite2: String,changeType:String,value1:String,value2:String,difference:String,count:String,siteList:String)
  case class GpsData(carNum:String,gpsTime:String,siteNum:String)
  case class GpsData2(carNum:String,gpsTime:String,siteNum:String,count:Int)
  case class GpsList(carNum:String,gpsTime:String,siteNum:String,count:Integer,siteList:String)
  case class NewIC(cardNo: String,cardType:String,getOnTime: String,getOnBusNo: String,getOnRouteID: String,getOnRoute: String,getOnSiteID: String,getOnSite: String,getOnSiteName: String,upOrDown: String,getOnLongitude: String,getOnLatitude: String,
                   changeType:String,value1:String,value2:String,count:String,siteList:String,targetRoute: String,targetSite: String,nextTime:String)
  case class TempResult(cardNo: String,cardType:String,getOnTime: String,getOnBusNo: String,getOnRouteID: String,getOnRoute: String,getOnSiteID: String,getOnSite: String,getOnSiteName: String,upOrDown: String,getOnLongitude: String,getOnLatitude: String,
                   changeType:String,value1:String,value2:String,count:String,siteList:String,getOffRoute: String,getOffSite: String,getOffTime:String,nextTime:String,distance:String)
  case class SingleOD(cardNo: String, getOnTime: String, getOnRouteID: String,getOnRoute: String,getOnSiteID:String, getOnSite: String, getOnSiteName: String,getOnLongitude: String, getOnLatitude: String,
                      getOffTime: String, getOffRouteID: String,getOffRoute: String,getOffSiteID:String, getOffSite: String, getOffSiteName: String, getOffLongitude: String, getOffLatitude: String,value1:String,value2:String)
  case class ChangeData(cardNo:String,preOnTime:String,preOffTime:String,preBusNo:String,preOnRouteID:String,preOffRouteID:String,preOnSiteID:String,preOffSiteID:String,preUpOrDown:String,preOnSiteName:String,preOffSiteName:String,
                      currOnTime:String,currOffTime:String,currBusNo:String,currOnRouteID:String,currOffRouteID:String,currOnSiteID:String,currOffSiteID:String,currUpOrDown:String,currOnSiteName:String,currOffSiteName:String)
  case class AllODResult(cardNo: String, getOnTime: String, getOnBusNo: String, getOnRouteID: String,getOnRoute: String,getOnSiteID:String, getOnSite: String, getOnSiteName: String, upOrDown: String, getOnLongitude: String, getOnLatitude: String,
                    getOffTime: String, getOffRouteID: String,getOffRoute: String,getOffSiteID:String, getOffSite: String, getOffSiteName: String, getOffLongitude: String, getOffLatitude: String,changeType:String,value1:String,value2:String)
}
