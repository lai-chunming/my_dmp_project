package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object ChannelTag extends Tags {
  /**
    * 渠道标签
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    //定义个一个数组存放结果
    val list=List[(String,Int)]()
    //如果类型匹配就类型转换成Row
    if (args(0).isInstanceOf[Row]) {
      val row: Row = args(0).asInstanceOf[Row]
      //获取广告商平台id
      val adplatformproviderid: String = row.getAs[String]("adplatformproviderid")
      if (StringUtils.isNotBlank(adplatformproviderid)) {
        list.:+("CN"+adplatformproviderid,1)
      }
      list
    }else{
      list
    }
  }
}
