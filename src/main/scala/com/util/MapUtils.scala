package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

object MapUtils {
  def getBusinessFromMap(long: Double, lat: Double):String = {
    //https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&key=b3287e684a2d0018f82c9089f6e29f85
    val location=long +","+lat
    val url="https://restapi.amap.com/v3/geocode/regeo?key=9f798bb4c95b76b99f8fac57524b1343&location="+location
    //调用http接口
    val response: String = MyHttpUtil.get(url)
    //返回的是json格式的字符串,解析
    val jSONObject: JSONObject = JSON.parseObject(response)
    val status: Int = jSONObject.getIntValue("status")
    //判断是否成功
    if(status==0) return ""
    //开始解析
    val regeocode = jSONObject.getJSONObject("regeocode")
    if(regeocode == null) return null
    val addressComponent = regeocode.getJSONObject("addressComponent")
    if(addressComponent == null) return null
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null) return null
    //对结果封装处理
    val result=ListBuffer[String]()
    for (value<-businessAreas.toArray) {
      if (value.isInstanceOf[JSONObject]) {
        val jObject: JSONObject = value.asInstanceOf[JSONObject]
        val name: String = jObject.getString("name")
        result.append(name)
      }
    }
    result.mkString(",")
  }

  def main(args: Array[String]): Unit = {
    val str: String = getBusinessFromMap(116.310003,39.991957)
    println(str)
  }
}
