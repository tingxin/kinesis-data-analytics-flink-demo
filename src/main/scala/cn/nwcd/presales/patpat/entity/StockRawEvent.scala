package cn.nwcd.presales.patpat.entity

import cn.nwcd.presales.common.util.JsonUtils
import com.fasterxml.jackson.annotation.JsonInclude

import java.text.SimpleDateFormat


@JsonInclude(JsonInclude.Include.NON_NULL)
case class StockRawEvent(
                          event_time: String,
                          name: String,
                          price: Double
                        )


object StockRawEvent {
  val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def ts(input: StockRawEvent): Long = {
    df.parse(input.event_time).getTime
  }
}