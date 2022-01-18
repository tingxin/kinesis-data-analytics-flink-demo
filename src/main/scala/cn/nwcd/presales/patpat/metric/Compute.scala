package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkCompute, FlinkContext}
import cn.nwcd.presales.patpat.entity.OrderRawEvent
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
    val rawEventDs: DataStream[OrderRawEvent] = getDataSet[DataStream[OrderRawEvent]]("input_events")
    val dwdDs = rawEventDs.filter(item => item.amount < 1000)

    setDataSet("dwd_orders", dwdDs)

    // 指标计算
    // 累计金额
    val totalAmountDs = dwdDs
      .map(item => Tuple2("key", item.amount))
      .keyBy(item => item._1)
      .reduce((item1, item2) => {
        Tuple2("key", item1._2 + item2._2)
      })
      .map(item => item._2)

    setDataSet("total_amount", totalAmountDs)

  }
}
