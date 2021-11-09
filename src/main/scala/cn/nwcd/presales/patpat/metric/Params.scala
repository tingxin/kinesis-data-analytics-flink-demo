package cn.nwcd.presales.patpat.metric

object Params {
   val REGION = "ap-southeast-1"
   val StockInputStream = "mock-stock-price-ds"
   val StockInputStream2 = "mock-stock-tx-ds"
   val OutputS3SinkPath = "s3://ohla-workshop/kinesis/"
   val OutputFirehouse = "outputDeliveryStreamName"
}
