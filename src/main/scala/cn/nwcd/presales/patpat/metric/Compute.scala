package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkCompute, FlinkContext}
import cn.nwcd.presales.patpat.entity.{StockEvent, StockRawEvent, StockTxEvent}
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.Collector

import java.lang

trait Compute extends EventFlinkCompute {
  this: FlinkContext =>


  override def compute(): Unit = {
    super.compute()
    val rawStockEventDs: DataStream[StockRawEvent] = getDataSet[DataStream[StockRawEvent]]("stock_input_events")
    val rawStockTxEventDs: DataStream[StockTxEvent] = getDataSet[DataStream[StockTxEvent]]("stock_tx_events")
    val coStream = rawStockEventDs.coGroup(rawStockTxEventDs)
      .where(item => item.name)
      .equalTo(item => item.name)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))

    val result = coStream.apply(new CoGroupFunction[StockRawEvent, StockTxEvent, StockEvent] {
      override def coGroup(iterable: lang.Iterable[StockRawEvent], iterable1: lang.Iterable[StockTxEvent], out: Collector[StockEvent]): Unit = {
        import scala.collection.JavaConverters._
        val scalaT1 = iterable.asScala.toList
        val scalaT2 = iterable1.asScala.toList

        for (left <- scalaT1) {
          var flag = false // 定义flag，left流中的key在right流中是否匹配
          for (right <- scalaT2) {
            out.collect(StockEvent(left.name, left.event_time, right.tx_name, left.price, right.name, right.event_time))
            flag = true;
          }
          if (!flag) { // left流中的key在right流中没有匹配到，则给itemId输出默认值0L
            out.collect(StockEvent(left.name, left.event_time, "Not Found", left.price, "Not Found", "Not Found"))
          }
        }
      }
    })

    setDataSet("joined_ds", result)
  }
}
