package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtils {
  private val config=new JedisPoolConfig
  //最大空闲连接数
  config.setMaxIdle(10)
  //最大连接数
  config.setMaxTotal(20)
  //创建连接池
  private val pool=new JedisPool(config,"mini-sp",6379,10000)
  def getJedis() ={
    pool.getResource
  }

}
