package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkCompute, FlinkContext}
import cn.nwcd.presales.patpat.entity.{StockEvent, StockEventPre, StockRawEvent, StockTxEvent}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
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

    val join1 = coStream.apply(new CoGroupFunction[StockRawEvent, StockTxEvent, StockEvent] {
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
    }).disableChaining().name("join1")

    val join1W = join1.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[StockEvent]
        .withTimestampAssigner(
          new SerializableTimestampAssigner[StockEvent] {
            override def extractTimestamp(t: StockEvent, l: Long): Long = {
              StockEvent.ts(t)
            }
          })).disableChaining().name("join1W")

    val thirdStream : DataStream[StockRawEvent] = rawStockEventDs.map(item=> StockRawEvent(item.event_time, item.name, item.price*2))

    val coStream2 = join1W.coGroup(thirdStream)
      .where(item => item.name)
      .equalTo(item => item.name)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))

    val join2 = coStream2.apply(new CoGroupFunction[StockEvent, StockRawEvent, StockEventPre] {
      override def coGroup(iterable: lang.Iterable[StockEvent], iterable1: lang.Iterable[StockRawEvent], out: Collector[StockEventPre]): Unit = {
        import scala.collection.JavaConverters._
        val scalaT1 = iterable.asScala.toList
        val scalaT2 = iterable1.asScala.toList

        for (left <- scalaT1) {
          var flag = false // 定义flag，left流中的key在right流中是否匹配
          for (right <- scalaT2) {
            out.collect(StockEventPre(left.name, left.event_time,left.tx_name,left.price,left.check_name,left.check_event_time,right.price))
            flag = true;
          }
          if (!flag) { // left流中的key在right流中没有匹配到，则给itemId输出默认值0L
            out.collect(StockEventPre(left.name, left.event_time,left.tx_name,left.price,left.check_name,left.check_event_time,0))
        }
      }
    }}).disableChaining().name("join2")

    setDataSet("joined_ds", join2)
  }
}
