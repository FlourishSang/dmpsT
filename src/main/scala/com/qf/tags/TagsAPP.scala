package com.qf.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 打APP标签
  */
object TagsAPP extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val appdic = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    // 获取APPname,appid
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      list:+=("APP"+appdic.value.getOrElse(appid,appid),1)
    }

    list
  }
}
