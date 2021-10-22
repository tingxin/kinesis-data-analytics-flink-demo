package cn.nwcd.presales.patpat.app

import cn.nwcd.presales.common.struct.FlinkApp
import cn.nwcd.presales.patpat.metric._

object MetricApp extends FlinkApp
  with Context
  with Input
  with Compute
  with Output