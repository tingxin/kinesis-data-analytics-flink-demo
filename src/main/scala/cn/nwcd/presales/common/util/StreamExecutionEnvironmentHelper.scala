package cn.nwcd.presales.common.util

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util


object StreamExecutionEnvironmentHelper {
  def isLocalEnvironment(env: StreamExecutionEnvironment): Boolean = {
    if (env.getJavaEnv.isInstanceOf[LocalStreamEnvironment]) {
      true
    } else {
      false
    }
  }

  def addGlobalConfiguration(env: StreamExecutionEnvironment, parameterTool: ParameterTool): Unit = {
    env.getConfig.setGlobalJobParameters(desensitize(parameterTool))
  }

  private def desensitize(parameterTool: ParameterTool): ParameterTool = {
    val result = new util.HashMap[String, String]()
    val properties = parameterTool.getProperties
    val iter = properties.entrySet().iterator();
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey.toString
      val sensitive = key.contains("password") || key.contains("passwd")
      if (!sensitive) {
        result.put(key, entry.getValue.toString)
      }
    }
    ParameterTool.fromMap(result)
  }
}
