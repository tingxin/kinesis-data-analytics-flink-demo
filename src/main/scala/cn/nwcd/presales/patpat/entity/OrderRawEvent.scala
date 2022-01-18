package cn.nwcd.presales.patpat.entity

import cn.nwcd.presales.common.util.JsonUtils
import com.fasterxml.jackson.annotation.JsonInclude

import java.text.SimpleDateFormat


@JsonInclude(JsonInclude.Include.NON_NULL)
case class OrderRawEvent(
                          create_time: Long,
                          update_time: Long,
                          order_id: Int,
                          email: String,
                          status: String,
                          good_count: Int,
                          city: String,
                          amount: Double,
                          business_key: String = "demo"
                        )

