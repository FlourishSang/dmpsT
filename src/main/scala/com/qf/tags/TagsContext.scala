package com.qf.tags

import com.qf.utils.{JedisConnectionPool, TagUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签，用来将所有标签进行汇总
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //模拟企业级编程  首先判断目录是否为空
    if (args.length != 5) {
      println("目录不正确，退出程序")
      sys.exit()
    }
    //创建一个集合存储输入输出目录
    val Array(inputPath, outputPath, dicPath, stopwords, day) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    //配置HBASE的基本信息
//    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.table.name")
//    // 配置HBASE的连接
//    val configuration = sc.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.host"))
//    val hbConn = ConnectionFactory.createConnection(configuration)
//    //获得操作对象
//    val hbadmin = hbConn.getAdmin
//    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
//      println("这个表可用！")
//      // 创建表对象
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      // 创建一个列簇
//      val columnDescriptor = new HColumnDescriptor("tags")
//      // 将列簇放入到表中
//      tableDescriptor.addFamily(columnDescriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbConn.close()
//    }
//    // 创建Job对象
//    val jobConf = new JobConf(configuration)
//    // 指定输出类型
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    // 指定输出表
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)


    // 获取字典文件并广播
    val dicMap = sc.textFile(dicPath).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collect.toMap
    val bdAppNameDic = sc.broadcast(dicMap)

    // 获取停用词库并广播
    val stopwordsDir = sc.textFile(stopwords).map((_, 0)).collect.toMap
    val bdstopWordsDic = sc.broadcast(stopwordsDir)

    // 读取数据
    val df = spark.read.parquet(inputPath)

    // 过滤需要的userId，因为userId很多，只需要过滤出userId不全为空的数据
    val filteredDF = df.filter(TagUtils.hasneedOneUserId)

    val concatTags: RDD[(String, List[(String, Int)])] =
      filteredDF.rdd.mapPartitions(part => {
        val jedis = JedisConnectionPool.getConnection()
        part.map(row => {
          // 处理一下userId，只拿到第一个不为空的userId作为这条数据的用户标识（userId）
          val userId = TagUtils.getAnyOneUserId(row)
          // 根据每一条数据  打上对应的标签信息（7种标签）
          // 开始打标签
          // 广告标签和渠道
          val adTag = TagsAd.makeTags(row)
          // APP标签
          val appTag = TagsAPP.makeTags(row, bdAppNameDic)
          // 设备
          val deviceTag = TagsDevice.makeTags(row)
          // 关键字
          val keywordTag = TagsKeyword.makeTags(row, bdstopWordsDic)
          // 地域
          val tagsLocation = TagsLocation.makeTags(row)
          // 商圈标签
          val Business = TagsBusiness.makeTags(row, jedis)

          (userId, adTag ++ appTag ++ deviceTag ++ keywordTag ++ tagsLocation) // ++ Business)
        })
      })

    // 进行聚合
    val aggrUserTags = concatTags.reduceByKey(
      (list1, list2) => {
        // (xxxx,List((ln爱奇艺，1),(lk0001,1),(ky武侠电影,1), ...)
        (list1 ::: list2).groupBy(_._1)
          .mapValues(_.foldLeft(0)(_ + _._2))
          .toList
      })

    // 测试：整理并输出数据
    aggrUserTags.map(t => {
      t._1 + "," + t._2.map(t => t._1 + ":" + t._2).mkString(",")
    }).saveAsTextFile(outputPath)

    // 写入到HBASE中
    //    aggrUserTags.map {
    //      case (userid, userTags) => {
    //        val put = new Put(Bytes.toBytes(userid))
    //        val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
    //        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$day"), Bytes.toBytes(tags))
    //
    //        (new ImmutableBytesWritable(), put)
    //      }
    //    }.saveAsHadoopDataset(jobConf)


    spark.stop()
  }
}
