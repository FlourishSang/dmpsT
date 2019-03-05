package com.qf.statement

import com.qf.utils.RptUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 媒体指标
  */
object AppRpt {
  def main(args: Array[String]): Unit = {
    if(args.length!= 3){
      println("目录不存在，请重新输入")
      sys.exit()
    }

    val Array(inputPath,outputPath,resultPath) = args

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // 读取字典文件
    val dicMap = sc.textFile(resultPath)
      .map(_.split("\t",-1))
      .filter(_.length>=5)
      .map(arr=>{
        // com.123.cn   爱奇艺
        (arr(4),arr(1))
      }).collect().toMap

    // 将文件广播出去
    val broadcast = sc.broadcast(dicMap)

    val df = spark.read.parquet(inputPath)

    df.rdd.map(row=>{
      // 获取媒体类别
      var appname = row.getAs[String]("appname")
      // 如果说我们取到空值的话，那么将取字典文件中进行查询
      if(StringUtils.isBlank(appname)){
        // 通过APPId获取字典文件中对应得APPid
        // 然后取到它的Value
        // com.123.cn   爱奇艺
        appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      }
      //val appname = broadcast.value.getOrElse(row.getAs[String]("appid"),"unknow")
      // 先把需要的字段拿出来，在进行操作
      // 处理 原始请求数，有效请求，广告请求
      val requestmode = row.getAs[Int]("requestmode") // 数据请求方式（1:请求、2:展示、3:点击）
      val processnode = row.getAs[Int]("processnode") // 流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
      // 参与竞价数，竞价成功数，展示数，点击数
      val iseffective = row.getAs[Int]("iseffective") // 有效标识
      val isbilling = row.getAs[Int]("isbilling") // 是否收费
      val isbid = row.getAs[Int]("isbid") // 是否rtb
      val iswin = row.getAs[Int]("iswin") // 是否竞价成功
      val adorderid = row.getAs[Int]("adorderid") // 广告id
      // 处理 广告成本 ，广告消费
      val winPrice = row.getAs[Double]("winprice") // rtb 竞价成功价格
      val adpayment = row.getAs[Double]("adpayment") // 转换后的广告消费
      // 调用业务的方法
      val reqlist = RptUtils.calculateReq(requestmode,processnode)
      val rtblist = RptUtils.caculateRtb(iseffective,
        isbilling,isbid,iswin,adorderid,winPrice,adpayment)
      val cliklist = RptUtils.calculateTimes(requestmode,iseffective)

      (appname, reqlist ++ rtblist ++ cliklist)
    }).reduceByKey((list1,list2) => {
      // list(0,2,1,5) list2(2,5,4,7)  zip((0,2),(2,5),(1,4),(5,7))
      (list1 zip list2).map(t => t._1 + t._2)
    }).map(t=>{
      t._1 + "," + t._2.mkString(",")
    }).take(10).toBuffer.foreach(println)

    sc.stop()
  }
}
