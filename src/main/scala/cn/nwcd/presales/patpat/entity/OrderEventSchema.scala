package cn.nwcd.presales.patpat.entity
import org.apache.flink.api.common.serialization.SerializationSchema


class OrderEventSchema() extends SerializationSchema[OrderRawEvent] {
  override def serialize(t: OrderRawEvent): Array[Byte] = t.toString.getBytes()
}