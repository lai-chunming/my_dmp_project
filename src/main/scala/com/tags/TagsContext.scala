package com.tags

import java.util.Properties

import com.etl.loadData
import com.util.{JedisUtils, TagsUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import redis.clients.jedis.Jedis

object TagsContext {
  /**
    * 标签上下文,合并所有标签
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val prop=new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("path.properties"))
    val inputPath: String = prop.getProperty("input_path.parquet")
    val dicPath: String = prop.getProperty("app_dic_path")
    val stopWordsPath=prop.getProperty("stop_words_path")
    val df: DataFrame = loadData.getParquetData(inputPath)
    //读取app的字典文件
    val spark: SparkSession = loadData.getSpark()
    //获取jedis
    val jedis: Jedis = JedisUtils.getJedis()
    val dic: RDD[String] = spark.sparkContext.textFile(dicPath)
    //广播
    val words: collection.Map[String, String] = dic.map(w => {
      w.split("\t", -1)
    }).filter(_.length >= 5)
      .map(ws => {
        (ws(4), ws(1))
      }).collectAsMap()
    val broadcastDic: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(words)
    //读取停用的词库
    val stopWords: collection.Map[String, Int] = spark.sparkContext.textFile(stopWordsPath).map((_,0)).collectAsMap()
    val broadcastSW: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(stopWords)
    //开始给数据打标签
    //对拿到的数据进行判断,保证至少有一个ID
    val filterdData: Dataset[Row] = df.filter(TagsUtils.uId)
    filterdData.rdd.map(row=>{
      //获取uid
      val userId: String = TagsUtils getUserId(row)
      //广告位类型标签
      val adSpaceTag: List[(String, Int)] = AdSpaceTag.makeTags(row)
      //渠道标签
      val channelTag: List[(String, Int)] = ChannelTag.makeTags(row)
      //app标签
      val appTag: List[(String, Int)] = APPTag.makeTags(row,broadcastDic)
      //设备标签
      val devTag: List[(String, Int)] = DevTag.makeTags(row)
      //关键字标签
      val keyWordTag: List[(String, Int)] = KeyWordTag.makeTags(row,broadcastSW)
      //位置标签
      val locationTag: List[(String, Int)] = LocationTag.makeTags(row)
      //商圈标签
      val businessTag: List[(String, Int)] = BusinessTag.makeTags(row,jedis)

    })
  }

}
