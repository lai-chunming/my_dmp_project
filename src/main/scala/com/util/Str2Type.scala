package com.util

object Str2Type {
  /**
    * 把String类型的数据转换成Int
    * @param string
    * @return
    */
  def toInt(string: String): Int ={
    try{
      string.toInt
    }catch {
      case exception: Exception=>
        0
    }
  }
  def toDouble(string: String): Double ={
    try{
      string.toDouble
    }catch{
      case exception: Exception=>
        0.0
    }
  }
}
