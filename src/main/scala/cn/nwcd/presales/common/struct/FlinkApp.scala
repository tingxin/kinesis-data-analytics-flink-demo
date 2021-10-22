package cn.nwcd.presales.common.struct

import cn.nwcd.presales.common.Constants

trait FlinkApp {
  this: FlinkContext
  with EventFlinkInput
  with EventFlinkCompute
  with EventFlinkOutput =>

  def initApp(args: Array[String]) = {
    logInfo("Start init app...")
    init(args)
  }

  def processApp(): Unit = {
    input()
    compute()
    output()
  }

  def execute() = {
    val name = if(getConfig.has(Constants.NAME)) {
      getConfig.getString(Constants.NAME)
    } else {
      "Flink App"
    }
    run(name)
  }


  def main(args: Array[String]): Unit = {
    initApp(args)
    processApp()
    execute()
  }

}
