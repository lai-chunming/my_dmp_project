package com.tags

import ch.hsr.geohash.GeoHash
import com.util.MapUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object BusinessTag extends Tags {
  /**
    * 商圈标签
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    val list = List[(String, Int)]()
    if (args(0).isInstanceOf[Row] &&args.length>=2 && args(1).isInstanceOf[Jedis]) {
      val row: Row = args(0).asInstanceOf[Row]
      //获取jedis
      val jedis: Jedis = args(1).asInstanceOf[Jedis]
      //判断一下读取的经纬度是否满足我国的范围(大概范围)
      if (row.getAs[String]("long").toDouble>=73
        && row.getAs[String]("long").toDouble<=135
        && row.getAs[String]("lat").toDouble >= 3
        && row.getAs[String]("lat").toDouble <= 53) {
        //获取经纬度
        val long: Double = row.getAs[Double]("long")
        val lat: Double = row.getAs[Double]("lat")
        //根据经纬度从第三方获取商圈
        getBusinessArea(long,lat,jedis)
      }
      list
    }else{
      list
    }
  }

  /**
    * 获取商圈
    * @param long
    * @param lat
    * @return
    */
  def getBusinessArea(long: Double, lat:Double,jedis:Jedis) = {
    //将经纬度转换成GeoHash编码
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    //从数据库查询该商圈是否存在
    var business: String = jedis.get(geoHash)
    //如果没有查到,则要从第三方获取
    if (business==null||business.length==0) {
      val business: String = MapUtils.getBusinessFromMap(long, lat)
      //将结果存到redis
      if (business!=null && business.length>0) {
        jedis.set(geoHash,business)
      }
    }
    business
  }
}
