package com.rpt

import java.util.Properties

import com.etl.loadData
import org.apache.spark.sql.DataFrame

object TerminalRpt {
  /**
    *终端设备分布
    * @param args
    */

  def main(args: Array[String]): Unit = {
    val prop=new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("path.properties"))
    val inputPath: String = prop.getProperty("input_path.parquet")
    val df: DataFrame = loadData.getParquetData(inputPath)
    val spark=loadData.getSpark()
    df.createTempView("source")
    //按运营分类
    val sql_isp=
      """
        |select ispname,
        |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end) srcRequest,
        |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end) validRequest,
        |sum(case when requestmode =1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 then 1 else 0 end) partAd,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1 and adorderid !=0 then 1 else 0 end) susAd,
        |sum(case when requestmode =2 and iseffective=1 then 1 else 0 end) showAd,
        |sum(case when requestmode =3 and iseffective=1 then 1 else 0 end) clicks,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) priCost,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adPay
        |from source group by ispname
      """.stripMargin
    //按网络分类
    val sql_networkmanner=
      """
        |select networkmannername,
        |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end) srcRequest,
        |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end) validRequest,
        |sum(case when requestmode =1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 then 1 else 0 end) partAd,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1 and adorderid !=0 then 1 else 0 end) susAd,
        |sum(case when requestmode =2 and iseffective=1 then 1 else 0 end) showAd,
        |sum(case when requestmode =3 and iseffective=1 then 1 else 0 end) clicks,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) priCost,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adPay
        |from source group by networkmannername
      """.stripMargin
    //按设备分类
    val sql_devicetype=
      """
        |select (case tmp.devicetype when 1 then '手机' when 2  then '平板' else '未知' end)ma,
        |tmp.srcRequest,tmp.validRequest,tmp.adRequest,tmp.partAd,tmp.susAd,tmp.showAd,tmp.clicks,tmp.priCost,tmp.adPay
        |from
        |(select devicetype,
        |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end) srcRequest,
        |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end) validRequest,
        |sum(case when requestmode =1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 then 1 else 0 end) partAd,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1 and adorderid !=0 then 1 else 0 end) susAd,
        |sum(case when requestmode =2 and iseffective=1 then 1 else 0 end) showAd,
        |sum(case when requestmode =3 and iseffective=1 then 1 else 0 end) clicks,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) priCost,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adPay
        |from source group by devicetype) tmp
      """.stripMargin
    //按操作系统分类
    val sql_client=
      """
        |select (case tmp.client when 1 then 'android' when 2  then 'ios' when 3 then 'wp' else 'unKnow' end)os,
        |tmp.srcRequest,tmp.validRequest,tmp.adRequest,tmp.partAd,tmp.susAd,tmp.showAd,tmp.clicks,tmp.priCost,tmp.adPay
        |from
        |(select client,
        |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end) srcRequest,
        |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end) validRequest,
        |sum(case when requestmode =1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 then 1 else 0 end) partAd,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1 and adorderid !=0 then 1 else 0 end) susAd,
        |sum(case when requestmode =2 and iseffective=1 then 1 else 0 end) showAd,
        |sum(case when requestmode =3 and iseffective=1 then 1 else 0 end) clicks,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) priCost,
        |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adPay
        |from source group by client)tmp
      """.stripMargin
     spark.sql(sql_client).show(5)
    //spark.sql(sql).write.partitionBy("provincename","cityname").json(prop.getProperty("locationRpt_output_path"))
    spark.stop()
  }
}
