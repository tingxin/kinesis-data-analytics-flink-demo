package cn.nwcd.presales.patpat.entity

import java.text.SimpleDateFormat

case class StockEvent(name: String,
                      event_time: String,
                      tx_name: String,
                      price: Double,
                      check_name:String,
                      check_event_time:String)

object StockEvent {
  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def ts(input: StockEvent): Long = {
    df.parse(input.event_time).getTime
  }
}