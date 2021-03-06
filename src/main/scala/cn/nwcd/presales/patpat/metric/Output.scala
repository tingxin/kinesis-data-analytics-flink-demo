package cn.nwcd.presales.patpat.metric

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer
import cn.nwcd.presales.common.struct.{EventFlinkOutput, FlinkContext}
import cn.nwcd.presales.patpat.entity.{StockEvent, StockEventPre, StockRawEvent}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.connector.jdbc.JdbcConnectionOptions
import org.apache.flink.connector.jdbc.JdbcExecutionOptions
import org.apache.flink.connector.jdbc.JdbcSink
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants

import java.sql.PreparedStatement
import java.util.Properties

trait Output extends EventFlinkOutput {
  this: FlinkContext =>

  override def output(): Unit = {
    super.output()
    val ds: DataStream[StockEventPre] = getDataSet[DataStream[StockEventPre]]("joined_ds")
    output2S3(ds)

  }

  def output2S3(ds: DataStream[StockEventPre]):Unit = {
    val strDs = ds.map(item=>item.toString).disableChaining().name("toText")
    val sink:StreamingFileSink[String] = StreamingFileSink.forRowFormat(new Path(Params.OutputS3SinkPath),
      new SimpleStringEncoder[String]("UTF-8")).build()
    strDs.addSink(sink).name("saveToS3")
  }


   def output2Firehouse(ds: DataStream[StockEventPre]) = {

    val inputProperties = new Properties
     inputProperties.setProperty("aws.region", Params.REGION)

    val sink = new FlinkKinesisFirehoseProducer[String](Params.OutputFirehouse, new SimpleStringSchema(), inputProperties)
     val strDs = ds.map(item=>item.toString).disableChaining().name("toText")
     strDs.addSink(sink).name("saveToFirehouse")
  }

}
