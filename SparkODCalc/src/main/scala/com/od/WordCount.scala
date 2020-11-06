package com.od

import java.io.{File, FileInputStream}

import com.od.Helper.TempResult
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.xml.XML
import java.io.UnsupportedEncodingException
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("WordCount")
      .master("local")
      .getOrCreate()
    //val path=args(0)
    import spark.implicits._
    var jarWholePath = this.getClass.getProtectionDomain.getCodeSource.getLocation.getFile
    try jarWholePath = java.net.URLDecoder.decode(jarWholePath, "UTF-8")
    catch {
      case e: UnsupportedEncodingException =>
        System.out.println(e.toString)
    }
    val jarPath = new File(jarWholePath).getParentFile.getAbsolutePath
    val xmlPath=jarPath+File.separator+"config.xml"
    val xml=XML.load(xmlPath)
    val busSitePath=(xml\"busSitePath").text                          //公交站点路径
    val railSitePath=(xml\"railSitePath").text                        //轨道站点路径
    val busPath=(xml\"busPath").text                        //轨道站点路径
    val siteNearSitePath=(xml\"siteNearSitePath").text                //站点临近站点数据路径
    val historyPath=(xml\"historyPath").text                          //历史记录路径

    println(busSitePath)
    println(railSitePath)
    println(busPath)
    println(siteNearSitePath)
    spark.stop()
  }
}
