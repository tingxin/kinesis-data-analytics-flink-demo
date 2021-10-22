package cn.nwcd.presales.common.struct

import cn.nwcd.presales.common.util.Logging

trait EventFlinkOutput extends Logging {
  def output(): Unit = {
    logInfo("Start EventFlinkOutput ...")
  }
}
