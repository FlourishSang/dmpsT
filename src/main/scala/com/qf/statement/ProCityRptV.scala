package com.qf.statement

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 统计各省市数据量分布情况 存入MySQL
  */
object ProCityRptV {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录不存在，请重新输入")
      sys.exit()
    }

    val inputPath = args(0)

    val conf = new SparkConf().setAppName(s"${this.getClass.getName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.sql.parquet.compression.codec", "snappy")

    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()

    // 读取存储好的parquet文件
    val df = spark.read.parquet(inputPath)

    // 注册临时表
    df.createOrReplaceTempView("log")

    // SQL操作数据
    val result = spark.sql("select provincename,cityname,count(*) cts from log " +
      "group by provincename,cityname")

    // 将结果以json格式写到磁盘目录
    // result.coalesce(1).write.json(outputPath)

    // 获取application.conf内容
    val load = ConfigFactory.load()

    // 设置请求mysql的配置信息
    val prop = new Properties()
    prop.setProperty("user", load.getString("jdbc.user"))
    prop.setProperty("password", load.getString("jdbc.password"))

    // 将数据存入jdbc
    result.write.mode(SaveMode.Overwrite).jdbc(
      load.getString("jdbc.url"), load.getString("jdbc.tablename"), prop)

    // 关闭
    spark.stop()
  }
}
