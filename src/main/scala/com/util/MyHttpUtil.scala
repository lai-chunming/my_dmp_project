package com.util
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils


object MyHttpUtil {
  /**
    * http请求
    * @param url
    */
  def get(url: String) = {
    //先获取http客户端
    val client = HttpClients.createDefault()
    //获取httpGet请求
    val httpGet = new HttpGet(url)
    //发送请求
    val response = client.execute(httpGet)
    // 做编码集限定
    EntityUtils.toString(response.getEntity,"UTF-8")
  }
}
