package cn.nwcd.presales.common.config

case class ConsumerConfig(
                           commonKafkaConfig: CommonKafkaConfig,
                           kafkaConsumerGroup: String,
                           kafkaAutoOffsetReset: String)

object ConsumerConfig extends ConfigChecker {
  val CONSUMER_GROUP_CONFIG = "kafka.consumer.groupId"
  val AUTO_OFFSET_RESET_CONFIG = "kafka.offset.auto.reset"

  def apply(implicit config: Parameters): ConsumerConfig = {
    ConsumerConfig(
      CommonKafkaConfig(config),
      getNecessaryConfigString(CONSUMER_GROUP_CONFIG),
      getNecessaryConfigString(AUTO_OFFSET_RESET_CONFIG)
    )
  }

  def getSensorConsumerConfig(implicit config: Parameters): ConsumerConfig = {
    ConsumerConfig(
      CommonKafkaConfig.getSensorConfig(config),
      getNecessaryConfigString(CONSUMER_GROUP_CONFIG),
      getNecessaryConfigString(AUTO_OFFSET_RESET_CONFIG)
    )
  }
}
