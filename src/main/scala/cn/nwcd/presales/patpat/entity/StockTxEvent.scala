package cn.nwcd.presales.patpat.entity

import com.fasterxml.jackson.annotation.JsonInclude

import java.text.SimpleDateFormat

@JsonInclude(JsonInclude.Include.NON_NULL)
case class StockTxEvent(
                         event_time: String,
                         name: String,
                         tx_name: String
                       )


object StockTxEvent {
  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def ts(input: StockTxEvent): Long = {
    df.parse(input.event_time).getTime
  }
}