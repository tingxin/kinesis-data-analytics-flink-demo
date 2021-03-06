package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkInput, FlinkContext}
import cn.nwcd.presales.patpat.entity.{StockRawEvent, StockTxEvent}
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants, ConsumerConfigConstants}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode

import java.util.Properties

trait Input extends EventFlinkInput {
  this: FlinkContext =>

  override def input(): Unit = {
    val applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties
    val flinkProperties = applicationProperties.get("FlinkApplicationProperties")
    if (flinkProperties == null) throw new RuntimeException("Unable to load FlinkApplicationProperties properties from the Kinesis Analytics Runtime.")
    val beginTimeStamp = flinkProperties.getProperty("beginTimeStamp")
    print("eg for get app property %s".format(beginTimeStamp))


    val inputProperties = new Properties
    inputProperties.setProperty("aws.region", Params.REGION)
    inputProperties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO")
    inputProperties.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS, "500")
    inputProperties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

    val kinesisConsumer = new FlinkKinesisConsumer[String](Params.StockInputStream, new SimpleStringSchema, inputProperties)
    val kinesisConsumer2 = new FlinkKinesisConsumer[String](Params.StockInputStream2, new SimpleStringSchema, inputProperties)

    val kinesisStream = env.addSource(kinesisConsumer)
      .disableChaining
      .name("stock_raw_events")

    val kinesisStream2 = env.addSource(kinesisConsumer2)
      .disableChaining
      .name("stock_tx_raw_events")

    val jsonParser = new ObjectMapper()

    val events = kinesisStream.map(item => {
      val jsonNode = jsonParser.readValue(item, classOf[JsonNode])
      val event_time = jsonNode.get("event_time").asText
      val name = jsonNode.get("name").asText
      val price = jsonNode.get("price").asDouble
      StockRawEvent(event_time, name, price)
    }).disableChaining().name("toJson")

    val events2 = kinesisStream2.map(item => {
      val jsonNode = jsonParser.readValue(item, classOf[JsonNode])
      val event_time = jsonNode.get("event_time").asText
      val name = jsonNode.get("name").asText
      val tx_name = jsonNode.get("tx_name").asText
      StockTxEvent(event_time, name, tx_name)
    }).disableChaining().name("toJson")

    val watermarkEvent = events.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[StockRawEvent]
        .withTimestampAssigner(
          new SerializableTimestampAssigner[StockRawEvent] {
            override def extractTimestamp(t: StockRawEvent, l: Long): Long = {
              StockRawEvent.ts(t)
            }
          })).disableChaining().name("withWatermark")

    val watermarkEvent2 = events2.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[StockTxEvent]
        .withTimestampAssigner(
          new SerializableTimestampAssigner[StockTxEvent] {
            override def extractTimestamp(t: StockTxEvent, l: Long): Long = {
              StockTxEvent.ts(t)
            }
          })).disableChaining().name("withWatermark")

    setDataSet("stock_input_events", watermarkEvent)
    setDataSet("stock_tx_events", watermarkEvent2)
  }
}
