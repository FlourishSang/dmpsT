package com.qf.tags

trait Tags {
  /**
    * 数据标签接口
    */
  def makeTags(args: Any*):List[(String,Int)]
}
