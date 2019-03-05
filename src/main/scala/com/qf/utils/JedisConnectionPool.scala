package com.qf.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


object JedisConnectionPool{
  val conf = new JedisPoolConfig()
  //最大连接数,
  conf.setMaxTotal(20)
  //最大空闲连接数
  conf.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  conf.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(conf, "node02", 6379, 10000)

  def getConnection(): Jedis = {
    pool.getResource
  }
}
