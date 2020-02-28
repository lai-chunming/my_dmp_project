package com.tags

trait Tags {
  /**
    * 定义一个标签接口
    */
  def makeTags(args:Any*):List[(String,Int)]

}
