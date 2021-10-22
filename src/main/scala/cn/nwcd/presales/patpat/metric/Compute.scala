package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkCompute, FlinkContext}
import cn.nwcd.presales.patpat.entity.StockRawEvent
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

trait Compute extends EventFlinkCompute {
  this: FlinkContext =>


  override def compute(): Unit = {
    super.compute()
    val rawStockEventDs: DataStream[StockRawEvent] = getDataSet[DataStream[StockRawEvent]]("stock_input_events")
    val result = rawStockEventDs
      .keyBy(item=>item.name)
      .timeWindow(Time.seconds(60))
      .max("price")
    setDataSet("max_price_ds", result)
  }
}
