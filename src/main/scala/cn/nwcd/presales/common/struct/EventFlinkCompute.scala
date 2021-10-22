package cn.nwcd.presales.common.struct

import cn.nwcd.presales.common.util.Logging

trait EventFlinkCompute extends Logging {
  def compute(): Unit = {
    logInfo("Start EventFlinkCompute ...")
  }

}
