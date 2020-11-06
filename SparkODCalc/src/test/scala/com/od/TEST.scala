package com.od

import org.apache.spark.sql.SparkSession

object TEST {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("TEST")
      .getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")
    val allData=spark.read.option("header","true").csv("file:///F:/ODData/tempData/matchGetOnSiteData/2019-11-20")
    allData.filter("getOnSiteID!='null'").show(10000)
    //println(allData.count())
    spark.stop()
  }
}
