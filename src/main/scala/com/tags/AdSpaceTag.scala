package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AdSpaceTag extends Tags {
  /**
    * 广告位标签
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    //定义个一个数组存放结果
    val list=List[(String,Int)]()
    //如果类型匹配就类型转换成Row
    if (args(0).isInstanceOf[Row]) {
      val row: Row = args(0).asInstanceOf[Row]
      //获取广告位类型和广告位名字
      val adSpaceType: Int = row.getAs[Int]("adspacetype")
      val adSpaceName: String = row.getAs[String]("adspacetypename")
        adSpaceType match {
          case value if value>9 => list.:+("LC0"+value,1)
          case value if value>0&& value<9=> list.:+("LC"+value,1)
        }
      if (StringUtils.isNotBlank(adSpaceName)) {
        list.:+("LN"+adSpaceName,1)
      }
      list
    }else{
      list
    }

  }
}
