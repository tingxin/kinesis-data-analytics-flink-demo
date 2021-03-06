package cn.nwcd.presales.patpat.metric

object Params {
   val REGION = "cn-northwest-1"
   val StockInputStream = "mock-stock-price-ds"
   val StockInputStream2 = "mock-stock-tx-ds"
   val OutputS3SinkPath = "s3://demo-kinesis-output/minute-max-price-stock/"
   val OutputFirehouse = "outputDeliveryStreamName"
}
