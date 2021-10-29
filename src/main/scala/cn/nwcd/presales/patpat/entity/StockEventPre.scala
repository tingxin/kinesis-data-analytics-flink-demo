package cn.nwcd.presales.patpat.entity

import java.text.SimpleDateFormat

case class StockEventPre(name: String,
                         event_time: String,
                         tx_name: String,
                         price: Double,
                         check_name: String,
                         check_event_time: String,
                         double_price: Double)

object StockEventPre {
  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def ts(input: StockEventPre): Long = {
    df.parse(input.event_time).getTime
  }
}