package cn.nwcd.presales.common.struct

import cn.nwcd.presales.common.Constants
import cn.nwcd.presales.common.config.UnionConfig
import cn.nwcd.presales.common.enumerations.EnvType
import org.apache.flink.api.java.utils.ParameterTool
import cn.nwcd.presales.common.util.{Logging, StreamExecutionEnvironmentHelper}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

trait FlinkContext extends Logging {
  @transient protected var env: StreamExecutionEnvironment = _
  @transient protected var tEnv: StreamTableEnvironment = _
  @transient protected var config: UnionConfig = _

  protected val datasetContext: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
  implicit protected val fc = this

  def init(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    logInfo("Finished Params")
    config = UnionConfig(params)
    logInfo("FlinkContext finished init ...")
    initEnv()
    initTEnv()
    StreamExecutionEnvironmentHelper.addGlobalConfiguration(env, params)
  }

  def initEnv(): Unit = {
    logInfo("FlinkContext Init Env")
  }

  def initTEnv(): Unit = {
    logInfo("FlinkContext Init Table Env")
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, bsSettings)
  }

  def run(name: String = "Flink App"): Unit = {
    val envType = if(config.has(Constants.ENV)) {
      EnvType.withName(config.getString(Constants.ENV).toUpperCase())
    } else {
      EnvType.withName("STREAM")
    }
//    env.registerJobListener(JobMetaListener(name))
    print("**run " + envType.toString)

    envType match {
      case EnvType.STREAM =>
        var u:StreamExecutionEnvironment=env

        u.execute(name)
      case EnvType.TABLE =>
        tEnv.execute(name)
      case _ =>
        logInfo(s"Invalid Env Type $envType")
        throw new RuntimeException(s"Invalid Env Type $envType")
    }
    logInfo(s"Start to Run $name ...")
  }

  def getEnv: StreamExecutionEnvironment = env

  def setEnv(_env: StreamExecutionEnvironment) = {
    env = _env
  }

  def getTEnv: StreamTableEnvironment = tEnv

  def setTEnv(_tEnv: StreamTableEnvironment): Unit = {
    tEnv = _tEnv
  }

  def getConfig: UnionConfig = config

  def getDataSetContext = datasetContext

  def getDataSet[T](name:String): T = fc.getDataSetContext.get(name) match {
    case Some(ds) => ds.asInstanceOf[T]
    case _ =>
      logError(s"Lack of $name in Component")
      throw new RuntimeException(s"Lack of $name in Component")
  }

  def setDataSet(key: String, value: Any) = datasetContext += (key -> value)
}
