package cn.nwcd.presales.patpat.metric

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer
import cn.nwcd.presales.common.struct.{EventFlinkOutput, FlinkContext}
import cn.nwcd.presales.patpat.entity.{DoubleJsonSchema, OrderEventSchema, OrderRawEvent}
import cn.nwcd.presales.patpat.metric.Params.OutputIndexStream
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.apache.flink.streaming.connectors.kinesis.config.{AWSConfigConstants}
import java.util.Properties

trait Output extends EventFlinkOutput {
  this: FlinkContext =>

  override def output(): Unit = {
    super.output()

    val dwdDs: DataStream[OrderRawEvent] = getDataSet[DataStream[OrderRawEvent]]("dwd_orders")
    output2Firehouse(dwdDs)

    val amountDs: DataStream[Double] = getDataSet[DataStream[Double]]("total_amount")
    output2Kinesis(amountDs)

  }

  def output2Kinesis(ds: DataStream[Double]):Unit = {
    val producerConfig = new Properties()
    // Required configs
    producerConfig.put(AWSConfigConstants.AWS_REGION, Params.REGION)
//    producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
//    producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
    // Optional KPL configs
    producerConfig.put("AggregationMaxCount", "4294967295")
    producerConfig.put("CollectionMaxCount", "1000")
    producerConfig.put("RecordTtl", "30000")
    producerConfig.put("RequestTimeout", "6000")
    producerConfig.put("ThreadPoolSize", "15")

    val kinesis = new FlinkKinesisProducer[Double](new DoubleJsonSchema("amount"), producerConfig)
    kinesis.setFailOnError(true)
    kinesis.setDefaultStream(OutputIndexStream)
    kinesis.setDefaultPartition("0")

    ds.addSink(kinesis)
  }


   def output2Firehouse(ds: DataStream[OrderRawEvent]) = {

    val inputProperties = new Properties
     inputProperties.setProperty("aws.region", Params.REGION)

    val sink = new FlinkKinesisFirehoseProducer[OrderRawEvent](Params.OutputFirehouse, new OrderEventSchema(), inputProperties)
     ds.addSink(sink).name("saveToFirehouse")
  }

}
