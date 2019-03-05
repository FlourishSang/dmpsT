package com.qf.statement

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 地域指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录不存在，请重新输入")
      sys.exit()
    }

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()

    // 获取数据
    val df = spark.read.parquet(args(0))

    // 生成临时表
    df.createOrReplaceTempView("log")

    val sql =
      "select " +
        "provincename," +
        "cityname," +
        "sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ysrequest," +
        "sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yxrequest," +
        "sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) adrequest," +
        "sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cyrequest," +
        "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess," +
        "sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) shows," +
        "sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) clicks," +
        "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspcost," +
        "sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspapy " +
      "from log " +
      "group by provincename,cityname"

    // 执行sql
    val resDF: DataFrame = spark.sql(sql)

    // 设置用于请求mysql的配置信息
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", load.getString("jdbc.user"))
    prop.setProperty("password", load.getString("jdbc.password"))

    //将数据存入mysql
    resDF.write.mode(SaveMode.Overwrite)
      .jdbc(load.getString("jdbc.url"), load.getString("jdbc.tbn"), prop)

    spark.stop()
  }
}
