package cn.nwcd.presales.common.struct

import cn.nwcd.presales.common.util.Logging

trait EventFlinkInput extends Logging {
  def input(): Unit = {
    logInfo("Start EventFlinkInput...")
  }
}
