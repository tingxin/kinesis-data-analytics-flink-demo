package cn.nwcd.presales.patpat.entity

import org.apache.flink.api.common.serialization.SerializationSchema


class DoubleJsonSchema(var name: String) extends SerializationSchema[Double] {
  override def serialize(t: Double): Array[Byte] = {
    val index = IndexEvent(name, t)
    index.toString.getBytes()
  }
}