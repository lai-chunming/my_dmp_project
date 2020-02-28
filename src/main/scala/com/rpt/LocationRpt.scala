package com.rpt

import java.util.Properties

import com.etl.loadData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object LocationRpt {
  def main(args: Array[String]): Unit = {
    val spark=loadData.getSpark()
    val prop=new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("path.properties"))
    val df: DataFrame = spark.read.parquet(prop.getProperty("input_path.parquet"))
    df.createTempView("source")
    val sql=
      """
        |select provincename,cityname,
        |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end) srcRequest,
        |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end) validRequest,
        |sum(case when requestmode =1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 then 1 else 0 end) partAd,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1 and adorderid !=0 then 1 else 0 end) susAd,
        |sum(case when requestmode =2 and iseffective=1 then 1 else 0 end) showAd,
        |sum(case when requestmode =3 and iseffective=1 then 1 else 0 end) clicks,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) priCost,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adPay
        |from source group by provincename,cityname
      """.stripMargin
    spark.sql(sql).write.partitionBy("provincename","cityname").json(prop.getProperty("locationRpt_output_path"))
    spark.stop()

  }

}
