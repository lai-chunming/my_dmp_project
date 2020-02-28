package com.util

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
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
    spark.sql(sql).coalesce(1).write.json("E:/1.BigData/4.project/project_3/data/city")
  }

  def df2Mysql(df:DataFrame,spark:SparkSession): Unit ={
    df.createTempView("source")
    val sql=
      """
        |select count(*)as ct,provincename,cityname from source group by provincename,cityname
      """.stripMargin
    val properties=new Properties()
    properties.load(this.getClass().getClassLoader.getResourceAsStream("mysql.properties"))
    spark.sql(sql).write.mode(SaveMode.Append).jdbc(
      properties.getProperty("jdbc.url"),
      properties.getProperty("jdbc.table"),
      properties)
    spark.stop()
  }
  def log2parquet(df:DataFrame): Unit ={
    df.write.parquet("E:/1.BigData/4.project/project_3/data/dataFrame")
  }

}
