package com.qf.tags

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * 商圈标签
  */
object TagsBusiness extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    var list = List[(String, Int)]()
    if (row.getAs[String]("long").toDouble >= 73.66 && row.getAs[String]("long").toDouble <= 135.05 &&
      row.getAs[String]("lat").toDouble >= 3.86 && row.getAs[String]("lat").toDouble <= 53.55) {
      val lat = row.getAs[String]("lat")
      val long = row.getAs[String]("long")
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)
      val business = jedis.get(geoHash)
      business.split(";").foreach(t => list :+= (t, 1))
    }

    list
  }
}
