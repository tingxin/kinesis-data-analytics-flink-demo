package cn.nwcd.presales.common.config

trait ConfigChecker {
  def getNecessaryConfigString(key: String)(implicit config: Parameters): String = {
    if (!config.has(key)) {
      throw new IllegalArgumentException("%s must be configured.".format(key))
    }
    config.getString(key)
  }

  def getNecessaryConfigDouble(key: String)(implicit config: Parameters): Double = {
    if (!config.has(key)) {
      throw new IllegalArgumentException("%s must be configured.".format(key))
    }
    config.getDouble(key)
  }

  def getNecessaryConfigInt(key: String)(implicit config: Parameters): Int = {
    if (!config.has(key)) {
      throw new IllegalArgumentException("%s must be configured.".format(key))
    }
    config.getInt(key)
  }

  def getNecessaryConfigFloat(key: String)(implicit config: Parameters): Float = {
    if (!config.has(key)) {
      throw new IllegalArgumentException("%s must be configured.".format(key))
    }
    config.getFloat(key)
  }

  def getNecessaryConfigShort(key: String)(implicit config: Parameters): Short = {
    if (!config.has(key)) {
      throw new IllegalArgumentException("%s must be configured.".format(key))
    }
    config.getShort(key)
  }

  def getNecessaryConfigBoolean(key: String)(implicit config: Parameters): Boolean = {
    if (!config.has(key)) {
      throw new IllegalArgumentException("%s must be configured.".format(key))
    }
    config.getBoolean(key)
  }
}
