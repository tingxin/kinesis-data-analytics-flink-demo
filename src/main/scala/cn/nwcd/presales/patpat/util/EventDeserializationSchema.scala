package cn.nwcd.presales.patpat.util

import cn.nwcd.presales.common.util.JsonUtils
import cn.nwcd.presales.patpat.entity.StockRawEvent
import org.apache.flink.api.common.serialization.{AbstractDeserializationSchema, DeserializationSchema}

class EventDeserializationSchema extends AbstractDeserializationSchema[StockRawEvent]{
  override def deserialize(bytes: Array[Byte]): StockRawEvent = {

    val rawJsonStr = new String(bytes, "utf-8")
    val event: StockRawEvent = JsonUtils.fromJson[StockRawEvent](rawJsonStr)

    event
  }
}
