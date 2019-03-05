package com.qf.tags

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */
object TagsKeyword extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val stopwords = args(1).asInstanceOf[Broadcast[Map[String,Int]]] // 停用字典
    val kwds = row.getAs[String]("keywords").split("\\|")
    kwds.filter(word=>{
      word.length>=3 && word.length<=8 && !stopwords.value.contains(word)
    }).foreach(word=>{
      list:+=("K"+word,1)
    })

    list
  }
}
