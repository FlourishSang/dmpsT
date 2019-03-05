package com.qf.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 打广告标签和渠道标签
  */
object TagsAd extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    //创建一个集合用于返回
    var list = List[(String, Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v > 0 && v <= 9 => list :+= ("LC0" + v, 1)
    }
    // 获取广告类型名称
    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)) {
      list :+= ("LN" + adName, 1)
    }
    // 渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list :+= ("CN" + channel, 1)

    list
  }
}
