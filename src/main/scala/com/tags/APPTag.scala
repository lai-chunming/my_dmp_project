package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object APPTag extends Tags {
  /**
    * app标签
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    val list = List[(String, Int)]()
    if (args.length>=2 && args(0).isInstanceOf[Row]) {
      val row: Row = args(0).asInstanceOf[Row]
      val map: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]
      val appName: String = row.getAs[String]("appname")
      val appId: String = row.getAs[String]("appid")
      if (StringUtils.isNotBlank(appName)) {
        list.:+("APP"+appName,1)
      }else{
        list.:+("APP"+map.value.getOrElse(appId,"其他"),1)
      }
      list
    }else{
      list
    }
  }
}
