package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {

  /**
    * imei:移动设备标识码  mac:苹果设备的标识码 openudid:udid的第三方解决方案  androidid:安卓设备的唯一标识码 idfa:广告标识码
    */
     val uId: String =
      """
        imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !=''
    """.stripMargin

  /**
    * 获取userid
    * @param row
    * @return
    */
  def getUserId(row: Row) = {
    row match {
      case r if StringUtils.isNotBlank(row.getAs[String]("imei"))=>"IMEI:"+r.getAs[String]("imei")
      case r if StringUtils.isNotBlank(row.getAs[String]("mac"))=>"MAC:"+r.getAs[String]("mac")
      case r if StringUtils.isNotBlank(row.getAs[String]("openudid"))=>"OUID:"+r.getAs[String]("openudid")
      case r if StringUtils.isNotBlank(row.getAs[String]("androidid"))=>"AID:"+r.getAs[String]("androidid")
      case r if StringUtils.isNotBlank(row.getAs[String]("idfa"))=>"IDFA:"+r.getAs[String]("idfa")
    }
  }


}

