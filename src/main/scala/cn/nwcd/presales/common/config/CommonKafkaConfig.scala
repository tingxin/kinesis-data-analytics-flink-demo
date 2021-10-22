package cn.nwcd.presales.common.config

case class CommonKafkaConfig(kafkaBrokers: String, topic: String, kafkaUsername: String, kafkaPassword: String)

object CommonKafkaConfig extends ConfigChecker {
  val BROKERS_CONFIG = "kafka.brokers"
  val TOPIC_CONFIG = "kafka.topic"
  val USER_NAME_CONFIG = "kafka.username"
  val PASSWORD_CONFIG = "kafka.password"

  def apply(implicit config: Parameters): CommonKafkaConfig = {
    CommonKafkaConfig (
      getNecessaryConfigString(BROKERS_CONFIG),
      getNecessaryConfigString(TOPIC_CONFIG),
      getNecessaryConfigString(USER_NAME_CONFIG),
      getNecessaryConfigString(PASSWORD_CONFIG)
    )
  }

  def getSensorConfig(implicit config: Parameters): CommonKafkaConfig = {
    CommonKafkaConfig (
      getNecessaryConfigString(BROKERS_CONFIG),
      getNecessaryConfigString(TOPIC_CONFIG),
      null,
      null
    )
  }
}