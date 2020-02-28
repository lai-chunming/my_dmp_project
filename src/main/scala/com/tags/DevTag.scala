package com.tags

import org.apache.spark.sql.Row

object DevTag extends Tags {
  /**
    * 设备标签
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    val list = List[(String, Int)]()
    if (args(0).isInstanceOf[Row]) {
      val row: Row = args(0).asInstanceOf[Row]
      //设备操作系统
      /**
        * 1 Android D00010001
        * 2 IOS D00010002
        * 3 WinPhone D00010003
        * _ 其 他 D00010004
        */
      val client: Int = row.getAs[Int]("client")
      client match{
        case 1 =>list.:+("Android D00010001",1)
        case 2 =>list.:+("IOS D00010002",1)
        case 3 =>list.:+("WinPhone D00010003",1)
        case _ =>list.:+("其 他 D00010004",1)
      }
      //联网方式
      /**
        * 设 备 联 网 方 式
        * WIFI D00020001
        * 4G D00020002
        * 3G D00020003
        * 2G D00020004
        * _  D00020005
        */
      val networkmannername: String = row.getAs[String]("networkmannername")
      networkmannername match{
        case "WIFI" =>list.:+("D00020001",1)
        case "4G" =>list.:+("D00020002",1)
        case "3G" =>list.:+("D00020003",1)
        case "2G" =>list.:+("D00020004",1)
        case _ =>list.:+("D00020005",1)
      }
      //运营商方式
      /*
      设备运营商方式
      移 动 D00030001
      联 通 D00030002
      电 信 D00030003
       _ D00030004
       */
      val ispName: String = row.getAs[String]("ispname")
      ispName match{
        case "移动"=>list.:+("D00030001",1)
        case "联通"=>list.:+("D00030002",1)
        case "电信"=>list.:+("D00030003",1)
        case _=>list.:+("D00030004",1)
      }
      list
    }else{
      list
    }
  }
}
