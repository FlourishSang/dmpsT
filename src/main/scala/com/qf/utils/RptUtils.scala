package com.qf.utils

object RptUtils {
  /**
    * 处理原始请求数、有效请求、广告请求
    *
    * @param reqMode
    * @param prcMode
    * @return
    */
  def calculateReq(reqMode: Int, prcMode: Int): List[Double] = {
    if (reqMode == 1 && prcMode == 1) {
      List[Double](1, 0, 0)
    } else if (reqMode == 1 && prcMode == 2) {
      List[Double](1, 1, 0)
    } else if (reqMode == 1 && prcMode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }

  /**
    * 处理参与竞价数、竞价成功数、广告消费、广告成本
    *
    * @param fective
    * @param bill
    * @param bid
    * @param win
    * @param adorderid
    * @param winPrice
    * @param adPayment
    * @return
    */
  def caculateRtb(fective: Int, bill: Int, bid: Int,
                  win: Int, adorderid: Int, winPrice: Double, adPayment: Double): List[Double] = {
    if (fective == 1 && bill == 1 && bid == 1) {
      List[Double](1, 0, 0, 0)
    } else if (fective == 1 && bill == 1 && win == 1 && adorderid != 0) {
      List[Double](0, 1, winPrice / 1000.0, adPayment / 1000.0)
    } else {
      List[Double](0, 0, 0, 0)
    }
  }

  /**
    * 处理展示数、点击数
    *
    * @param reqMode
    * @param fective
    * @return
    */
  def calculateTimes(reqMode: Int, fective: Int): List[Double] = {
    if (reqMode == 2 && fective == 1) {
      List[Double](1, 0)
    } else if (reqMode == 3 && fective == 1) {
      List[Double](0, 1)
    } else {
      List[Double](0, 0)
    }
  }
}
