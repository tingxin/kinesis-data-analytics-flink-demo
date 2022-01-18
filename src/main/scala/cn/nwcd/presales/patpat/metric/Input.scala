package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.{EventFlinkInput, FlinkContext}
import cn.nwcd.presales.patpat.entity.{OrderRawEvent}
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

    val kinesisConsumer = new FlinkKinesisConsumer[String](Params.InputStream, new SimpleStringSchema, inputProperties)
    val kinesisStream = env.addSource(kinesisConsumer)
      .disableChaining
      .name("raw_events")


    val jsonParser = new ObjectMapper()

    val events = kinesisStream.map(item => {
      val jsonNode = jsonParser.readValue(item, classOf[JsonNode])
      val create_time = jsonNode.get("create_time").asLong
      val update_time = jsonNode.get("update_time").asLong
      val order_id = jsonNode.get("order_id").asInt
      val email = jsonNode.get("user_mail").asText
      val status = jsonNode.get("status").asText
      val good_count = jsonNode.get("good_count").asInt
      val city = jsonNode.get("city").asText
      val amount = jsonNode.get("amount").asDouble
      OrderRawEvent(create_time, update_time,order_id,email,status,good_count,city,amount)
    }).disableChaining().name("toJson")


    val watermarkEvent = events.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[OrderRawEvent]
        .withTimestampAssigner(
          new SerializableTimestampAssigner[OrderRawEvent] {
            override def extractTimestamp(t: OrderRawEvent, l: Long): Long = {
              t.update_time
            }
          })).disableChaining().name("withWatermark")



    setDataSet("order_input_events", watermarkEvent)
  }
}
