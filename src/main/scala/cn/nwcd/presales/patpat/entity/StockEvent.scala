package cn.nwcd.presales.patpat.entity

case class StockEvent(name: String,
                      event_time: String,
                      tx_name: String,
                      price: Double,
                      check_name:String,
                      check_event_time:String)
