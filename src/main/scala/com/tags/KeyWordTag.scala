package com.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeyWordTag extends Tags {
  /**
    * 定义一个标签接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    val list = List[(String, Int)]()
    if (args(0).isInstanceOf[Row] && args.length>=2) {
      val row: Row = args(0).asInstanceOf[Row]
      val map: Broadcast[collection.Map[String, Int]] = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
      //解析关键字
      /*
      关键字（标签格式：Kxxx->1）xxx 为关键字，关键字个数不能少于 3 个字符，且不能
       超过 8 个字符；关键字中如包含‘‘|’’，则分割成数组，转化成多个关键字标签
       另外还要满足不包含停止使用的词库
       */
      val keyWords: Array[String] = row.getAs[String]("keywords").split("\\|")
      keyWords.filter(word=>{
        word.length>=3 && word.length<=8 && !map.value.contains(word)
      }).foreach(word=>list.:+("K"+word,1))
      list
    }else{
      list
    }
  }
}
