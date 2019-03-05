package com.qf.statement

import com.qf.beans.Log
import com.qf.utils.{JedisConnectionPool, RptUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体指标(第二种方式)
  */
object AppRptV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("argument is wrong!!")
      sys.exit()
    }
    val Array(inputPath, dicPath, resultPath) = args
    val conf = new SparkConf().setMaster("local[*]")
      .setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.textFile(inputPath).map(_.split(",", -1))
      .filter(_.length >= 85)
      .map(Log(_))
      .mapPartitions(itr => {
        val jedis = JedisConnectionPool.getConnection()
        val results = new collection.mutable.ListBuffer[(String, List[Double])]()
        itr.foreach(log => {
          var appname = log.appname
          if (StringUtils.isBlank(appname)) {
            appname = jedis.get(log.appid)
          }
          val reqlist = RptUtils.calculateReq(log.requestmode, log.processnode)
          val rtblist = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
          val clicklist = RptUtils.calculateTimes(log.requestmode, log.iseffective)
          results += ((appname, reqlist ++ rtblist ++ clicklist))
        })
        jedis.close()
        // 将ListBuffer[(String,List[Double])]类型转换成为Iterator[(String,List[Double])]
        results.iterator
      }).reduceByKey((list1, list2) => {
      // 比如：List1(0,2,3,4,2)和List2(2,3,4,2,1)
      // zip后结果：List((0,2),(2,3),(3,4))
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1 + "," + t._2.mkString(","))
      .saveAsTextFile(resultPath)

    sc.stop()
  }
}
