package cn.nwcd.presales.patpat.metric

import com.amazonaws.services.kinesisanalytics.flink.connectors.producer.FlinkKinesisFirehoseProducer
import cn.nwcd.presales.common.struct.{EventFlinkOutput, FlinkContext}
import cn.nwcd.presales.patpat.entity.{DoubleJsonSchema, OrderEventSchema, OrderRawEvent}
import cn.nwcd.presales.patpat.metric.Params.OutputIndexStream
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants

import java.util.Properties

trait Output extends EventFlinkOutput {
  this: FlinkContext =>

  override def output(): Unit = {
    super.output()
    
    val amountDs: DataStream[Double] = getDataSet[DataStream[Double]]("total_amount")
    outputKafka(amountDs)

  }

  def outputKafka(ds: DataStream[Double]):Unit = {
    val brokerList = "b-1.demo-cluster-1.9z77lu.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-2.demo-cluster-1.9z77lu.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-3.demo-cluster-1.9z77lu.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092"
    val kafka = new FlinkKafkaProducer[Double](brokerList,"metric",new DoubleJsonSchema("amount"))
    ds.addSink(kafka)
  }

}
