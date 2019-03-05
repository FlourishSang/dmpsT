package com.qf.graph

import com.qf.tags._
import com.qf.utils.TagUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签，用来将所有标签进行汇总
  */
object TagsTodayFinal {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("目录不正确，退出程序")
      sys.exit()
    }

    // 接收参数
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


//    // 配置HBASE的基本信息
//    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.table.name")
//    // 配置HBASE的连接
//    val configuration = sc.hadoopConfiguration
//    configuration.set("hbase.zookeeper.quorum",
//      load.getString("hbase.zookeeper.host"))
//    val hbConn = ConnectionFactory.createConnection(configuration)
//    //获得操作对象
//    val hbadmin = hbConn.getAdmin
//    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
//      // 创建表对象
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
//      //创建一个列簇
//      val columnDescriptor = new HColumnDescriptor("tags")
//      //将列簇放入到表中
//      tableDescriptor.addFamily(columnDescriptor)
//      hbadmin.createTable(tableDescriptor)
//      hbadmin.close()
//      hbConn.close()
//    }
//    // 创建Job对象
//    val jobConf = new JobConf(configuration)
//    // 指定输出类型
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    // 指定输出到哪张表中
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

    // 读取字典文件
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
    val result = df.filter(TagUtils.hasneedOneUserId)
      .rdd.map(row => {
      // 处理下用户信息
      val userId: List[String] = TagUtils.getRowAllUserId(row)
      (userId, row)
    })

    // 构建点集合
    val vre = result.flatMap(tp => {
      val row = tp._2
      // 根据每一条数据  打上对应得标签信息（7种标签）
      // 开始打标签
      // 广告标签 和 渠道
      val adTag = TagsAd.makeTags(row)
      // APP标签
      val appTag = TagsAPP.makeTags(row, bdAppNameDic)
      // 设备
      val deviceTag = TagsDevice.makeTags(row)
      // 关键字
      val keywordTag = TagsKeyword.makeTags(row, bdstopWordsDic)
      // 地域
      val tagsLocation = TagsLocation.makeTags(row)
      // 商圈为什么没打，因为数据不准确，如果打商圈的话，那么将出现问题，啥数据都没有
      // 所以 商圈舍弃一下，但是商圈逻辑代码没问题
      // 将当前所打上的标签数据放到一起
//      val Business = TagsBusiness.makeTags(row, jedis)

      val currentRowTag = adTag ++ tagsLocation ++ deviceTag ++ appTag ++ keywordTag
      // List[String]    List[(String.Int)]
      val vd: List[(String, Int)] = tp._1.map((_, 0)) ++ currentRowTag
      // 只有第一个人可以携带顶点VD，其他不需要
      // 如果同一行上有多个顶点VD，因为同一行数据都有一个用户
      // 将来肯定要聚合在一起的，这样就会造成重复的叠加了
      tp._1.map(uId => {
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, vd)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })
    vre.take(20).foreach(println)

    // 构建边的集合
    val edges: RDD[Edge[Int]] = result.flatMap(tp => {
      // a b c : a->b a->c
      tp._1.map(uid => {
        Edge(tp._1.head.hashCode.toLong, uid.hashCode.toLong, 0)
      })
    })
//    edges.take(20).foreach(println)

    //图计算
    val graph = Graph(vre, edges)
    // 调用连通图算法，找到图中可以连通的分支
    // 并取出每个连通分支中最小的点的元组集合
    val cc = graph.connectedComponents().vertices
    //cc.take(20).foreach(println)
    // 认祖归宗
    val joined = cc.join(vre).map {
      case (uid, (commonId, tagsAndUserid)) => (commonId, tagsAndUserid)
    }
    val res = joined.reduceByKey {
      case (list1, list2) =>
        (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }
    res.take(20).foreach(println)

    //写入到HBASE中
    //    res.map {
    //      case (userid, userTags) => {
    //        val put = new Put(Bytes.toBytes(userid))
    //        val tags = userTags.map(t => t._1 + ":" + t._2).mkString(",")
    //        put.addImmutable(Bytes.toBytes("tags"),
    //          Bytes.toBytes(s"$day"), Bytes.toBytes(tags))
    //        (new ImmutableBytesWritable(), put)
    //      }
    //    }.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}
