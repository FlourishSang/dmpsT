package com.qf.etl

import com.qf.beans.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

object Bz2Parquet2 {
  def main(args: Array[String]): Unit = {

    //模拟企业级编程  首先判断目录是否为空
    if(args.length != 2){
      println("目录不正确，退出程序")
      sys.exit()
    }

    //创建一个集合存储输入输出目录
    val Array(inputPath,outputPath) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    //开始读取数据
    val lines = spark.sparkContext.textFile(inputPath)

    //进行过滤，保证字段大于85，并且 需要解析内部的,,,,,, 要进行特殊处理
    val logRDD: RDD[Log] = lines
      .map(t => t.split(",", t.length))
      .filter(_.length >= 85)
      .map(Log(_))

    val df =spark.createDataFrame(logRDD)

    df.write.partitionBy("provincename","cityname").parquet(outputPath)

    spark.stop()
  }
}
