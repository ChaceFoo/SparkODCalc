package com.od

import com.od.OD_01_PreprocessData.preprocessBusSiteData
import org.apache.spark.sql.SparkSession

import scala.xml.XML

object OD_00_PreparingBasicData {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("OD_00_PreparingBasicData")
      .master("local")  //提交到集群需注释掉
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val xml=XML.load(this.getClass.getClassLoader.getResource("config.xml"))
    val busSitePath=(xml\"busSitePath").text                          //公交站点路径
    val railSitePath=(xml\"railSitePath").text                        //轨道站点路径
    val siteNearSitePath=(xml\"siteNearSitePath").text                //站点临近站点数据路径

    val busSiteData = preprocessBusSiteData(spark,busSitePath)


  }
}
