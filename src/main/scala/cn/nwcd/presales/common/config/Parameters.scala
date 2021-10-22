package cn.nwcd.presales.common.config

import scala.collection.mutable
import cn.nwcd.presales.common.util.Logging

case class Parameters(param:  scala.collection.mutable.Map[String, Any]) extends Logging {

  def has(key: String): Boolean = {
    param.contains(key)
  }
  def getString(key: String): String = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[String]
    }else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def getDouble(key: String): Double = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[Double]
    }else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def getFloat(key: String): Float = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[Float]
    }else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def getInt(key: String): Int = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[Int]
    }else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def getLong(key: String): Long = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[Long]
    }else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def getShort(key: String): Short = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[Short]
    }else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def getBoolean(key: String): Boolean = {
    if(param.contains(key)) {
      param.get(key).get.asInstanceOf[Boolean]
    } else {
      logInfo(s"Failed to find $key in Parameters")
      throw new RuntimeException(s"Failed to find $key in Parameters")
    }
  }

  def set(key: String, value: Any): Unit = {
    param.put(key, value)
  }
}

object Parameters {
  def apply(): Parameters = {
    val param = mutable.Map[String, Any]()
    Parameters(param)
  }
}
