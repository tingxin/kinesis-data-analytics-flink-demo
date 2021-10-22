package cn.nwcd.presales.patpat.metric

import cn.nwcd.presales.common.struct.FlinkContext
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.ZoneId
import java.util.concurrent.TimeUnit

trait Context extends FlinkContext {

  override def initEnv(): Unit = {
    super.initEnv()
    env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        5, // 尝试重启的次数，默认INT_MAX。如果重启次数达到restart-strategy.fixed-delay.attempts参数规定的阈值之后还没有成功，就停止Job
        Time.of(60, TimeUnit.SECONDS).toMilliseconds))
    //config checkpointing
    env.setParallelism(1)
    env.enableCheckpointing(30 * 1000)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // env.setStateBackend(new RocksDBStateBackend(config.getString(s"$FLINK_PREFIX.$CHECKPOINT_PATH")))
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getConfig.setLatencyTrackingInterval(30000) // latency sample interval 30s
    super.initTEnv()
    tEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
  }
}
