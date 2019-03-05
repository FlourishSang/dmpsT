package com.qf.statement

import com.qf.utils.RptUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 地域指标（spark core实现）
  */
object LocationRptV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录不存在，请重新输入")
      sys.exit()
    }

    val Array(inputPath, outputPath) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()

    // 获取数据
    val df = spark.read.parquet(inputPath)

    val provinceAndCityRDD: RDD[((String, String), List[Double])] = df.rdd.map(row => {
      // 先把需要的字段拿出来，再进行操作
      // 处理 原始请求数，有效请求，广告请求
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")

      // 参与竞价数， 竞价成功数，展示数，点击数
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")

      // 处理 广告消费，广告成本
      val winPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      // 调用业务的方法
      // 处理 原始请求数，有效请求，广告请求
      val reqlist = RptUtils.calculateReq(requestmode, processnode) // List(1,1,0)
      // 参与竞价数，竞价成功数，广告消费 ，广告成本
      val rtblist = RptUtils.caculateRtb(iseffective,
        isbilling, isbid, iswin, adorderid, winPrice, adpayment) // List(1,0,0,0)
      // 展示数，点击数
      val cliklist = RptUtils.calculateTimes(requestmode, iseffective) // List(0,1)

      // reqlist ++ rtblist ++ cliklist: List(0,1,0,1,0,0,0,0,1)
      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")), reqlist ++ rtblist ++ cliklist)
    })

    // 进行聚合
    // 比如：List(0,2,1,5) List2(2,5,4,7)
    // zip后结果：List((0,2),(2,5),(1,4),(5,7))
    // map后结果：List(2,7,5,12)
    val sumedRDD: RDD[((String, String), List[Double])] =
    provinceAndCityRDD.reduceByKey((list1, list2) => (list1 zip list2).map(t => t._1 + t._2))

    // 拼接值
    val concatRDD: RDD[String] = sumedRDD.map(t => t._1._1 + "," + t._1._2 + "," + t._2.mkString(","))

    concatRDD.saveAsTextFile(outputPath)

    spark.stop()
  }
}
