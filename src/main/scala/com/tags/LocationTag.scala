package com.tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object LocationTag extends Tags {
  /**
    * 位置标签
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    //定义个一个数组存放结果
    val list=List[(String,Int)]()
    //如果类型匹配就类型转换成Row
    if (args(0).isInstanceOf[Row]) {
      val row: Row = args(0).asInstanceOf[Row]
      //省份
      val provincename: Int = row.getAs[Int]("provincename")
      val cityname: String = row.getAs[String]("cityname")
      if (provincename!=null) {
        list.:+("ZP"+provincename,1)
      }else{
        list.:+("其他",1)
      }
      if (cityname!=null) {
        list.:+("ZP"+cityname,1)
      }else{
        list.:+("其他",1)
      }
      list
    }else{
      list
    }
  }
}
