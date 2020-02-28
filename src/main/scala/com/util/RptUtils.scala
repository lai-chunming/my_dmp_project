package com.util

object RptUtils {

  /**
    * 处理请求类
 *
    * @param requestmode
    * @param processnode
    * @return
    */
  def requestAd(requestmode: Int, processnode: Int) = {
    //返回格式应该是List(1,1,1)这种,分别代表:原始请求,有效请求,广告请求
    if (requestmode==1 && processnode==1) {
      List[Double](1,0,0)
    }else if (requestmode==1 && processnode==2) {
      List[Double](1,1,0)
    }else if (requestmode==1 && processnode==3) {
      List[Double](1,1,1)
    }else {
      List[Double](0,0,0)
    }
  }

  /**
    * 处理参与竞价和广告资金
    * @param iseffective
    * @param isbilling
    * @param isbid
    * @param iswin
    * @param adorderid
    * @param winprice
    * @param adpayment
    * @return
    */
  def adPrice(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int, adorderid: Int, winprice: Double, adpayment: Double)= {
    if (iseffective==1 && isbilling==1 && isbid==1) {
      if (iswin==1) {
        List[Double](1,1,winprice/1000,adpayment/1000)
      }else {
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }

  /**
    * 展示和点击量
    * @param requestmode
    * @param iseffective
    * @return
    */
  def shows(requestmode: Int, iseffective: Int)= {
    if (requestmode==2 && iseffective==1) {
      List[Double](1,0)
    }else if (requestmode==3 && iseffective==1) {
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }


}
