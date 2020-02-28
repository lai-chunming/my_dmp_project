package com.rpt

import java.util.Properties

import com.etl.loadData
import com.util.RptUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object AppRpt {
  /**
    * 媒体分布
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val prop=new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("path.properties"))
    val inputPath: String = prop.getProperty("input_path.parquet")
    val dicPath: String = prop.getProperty("app_dic_path")
    val df: DataFrame = loadData.getParquetData(inputPath)
    //读取app的字典文件
    val spark: SparkSession = loadData.getSpark()
    val dic: RDD[String] = spark.sparkContext.textFile(dicPath)
    val appMap = dic.map(words =>
      words.split("\t", -1)
    ).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collectAsMap()
    val appBroadcast = spark.sparkContext.broadcast(appMap)
    df.rdd.map(rdd=> {
      var appname = rdd.getAs[String]("appname")
      if (!StringUtils.isNoneBlank(appname)) {
        appname=appBroadcast.value.getOrElse(rdd.getAs[String]("appname"),"其他")
      }
      //使用core的方式统计的话,需要获取9个字段,也就是计算逻辑中用的字段
      val requestmode = rdd.getAs[Int]("requestmode")
      val processnode = rdd.getAs[Int]("processnode")
      val iseffective = rdd.getAs[Int]("iseffective")
      val isbilling = rdd.getAs[Int]("isbilling")
      val isbid = rdd.getAs[Int]("isbid")
      val iswin = rdd.getAs[Int]("iswin")
      val adorderid = rdd.getAs[Int]("adorderid")
      val winprice = rdd.getAs[Double]("winprice")
      val adpayment = rdd.getAs[Double]("adpayment")
      //对于计算方法中需要统计的9个指标,可以把它分为三类
      /*
      1.请求类
      2.参与竞价,广告资金类
      3.点击,展示类
       */
      val requestList: List[Double] = RptUtils.requestAd(requestmode,processnode)
      val adpriceList: List[Double] = RptUtils.adPrice(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val showList: List[Double] = RptUtils.shows(requestmode,iseffective)
      (appname,requestList:::adpriceList:::showList)
    }).reduceByKey((list1,list2)=>{
      list1.zip(list2).map(tp=>tp._1+tp._2)
    })
    spark.stop()
  }
}
