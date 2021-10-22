package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkOutput, FlinkContext}
import cn.nwcd.presales.patpat.entity.StockRawEvent
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

import java.util.Properties

trait Output extends EventFlinkOutput {
  this: FlinkContext =>

  override def output(): Unit = {
    super.output()
    val ds: DataStream[StockRawEvent] = getDataSet[DataStream[StockRawEvent]]("max_price_ds")
    val strDs = ds.map(item=>item.toString).disableChaining().name("toText")
    val sink:StreamingFileSink[String] = StreamingFileSink.forRowFormat(new Path(Params.OutputS3SinkPath),
      new SimpleStringEncoder[String]("UTF-8")).build()
    strDs.addSink(sink).name("save")
  }


}
