package com.util

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object saveDF {
  /**
    * 统计各省市数据量分布情况,将df数据以json的格式保存到磁盘
    * @param df
    * @param spark
    */
  def df2Json(df:DataFrame,spark:SparkSession): Unit ={
    df.createTempView("source")
    val sql=
      """
        |select count(*)as ct,provincename,cityname from source group by provincename,cityname
      """.stripMargin
    spark.sql(sql).write.json("E:/1.BigData/4.project/project_3/data/city")
  }

  def df2Mysql(df:DataFrame,spark:SparkSession): Unit ={
    df.createTempView("source")
    val sql=
      """
        |select count(*)as ct,provincename,cityname from source group by provincename,cityname
      """.stripMargin
    val url="jdbc:mysql://mini1-sp:3306/spark"
    val table="cnt_by_city"
    val properties=new Properties()
    properties.put("user","root")
    properties.put("password","950816")
    spark.sql(sql).write.mode(SaveMode.Append).jdbc(url,table,properties)
    spark.stop()
  }

}
