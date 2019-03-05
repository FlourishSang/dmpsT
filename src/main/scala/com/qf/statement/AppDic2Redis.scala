package com.qf.statement

import com.qf.utils.JedisConnectionPool
import org.apache.spark.{SparkConf, SparkContext}

object AppDic2Redis {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("argument is wrong!!")
      sys.exit()
    }
    val Array(inputPath) = args

    val conf = new SparkConf().setMaster("local[*]")
      .setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    sc.textFile(inputPath)
      .filter(_.split("\t").length >= 5)
      .map(line => {
        val fields = line.split("\t", -1)
        (fields(4), fields(1))
      })
      .foreachPartition(it => {
        val jedis = JedisConnectionPool.getConnection()
        it.foreach(t => {
          jedis.set(t._1, t._2)
        })
        jedis.close()
      })

    sc.stop()
  }
}
