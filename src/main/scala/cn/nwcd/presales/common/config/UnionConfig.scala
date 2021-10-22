package cn.nwcd.presales.common.config

import cn.nwcd.presales.common.Constants
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.java.utils.ParameterTool
import cn.nwcd.presales.common.util.Logging

import java.io.File

case class UnionConfig(argParams: ParameterTool, fileParams: Config) extends ConfigChecker with Logging with Serializable {
  def has(key: String): Boolean = {
    argParams.has(key) || (fileParams != null && fileParams.hasPath(key))
  }

  def getString(key: String): String = {
    if(argParams.has(key)) {
      argParams.get(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getString(key)
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getBoolean(key: String): Boolean = {
    if(argParams.has(key)) {
      argParams.getBoolean(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getBoolean(key)
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getDouble(key: String): Double = {
    if(argParams.has(key)) {
      argParams.getDouble(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getDouble(key)
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getFloat(key: String): Float = {
    if(argParams.has(key)) {
      argParams.getFloat(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getDouble(key).toFloat
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getInt(key: String): Int = {
    if(argParams.has(key)) {
      argParams.getInt(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getInt(key)
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getLong(key: String): Long = {
    if(argParams.has(key)) {
      argParams.getLong(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getLong(key)
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getShort(key: String): Short = {
    if(argParams.has(key)) {
      argParams.getShort(key)
    } else if (fileParams != null && fileParams.hasPath(key)) {
      fileParams.getInt(key).toShort
    } else {
      logError(s"Failed to get $key in config")
      throw new RuntimeException(s"Failed to get $key in config")
    }
  }

  def getArgParams = argParams
  def getFileParams = fileParams
}


object UnionConfig extends Logging{
  def apply(paramTool: ParameterTool): UnionConfig = {
    if(paramTool.has(Constants.CONFIG)) {
      val filePath = paramTool.get(Constants.CONFIG)
      val configFile = new File(filePath)
      if (!configFile.exists()) {
        logError(s"$filePath file can't be found")
      }
      val config = ConfigFactory.parseFile(configFile)
      UnionConfig(paramTool, config)
    } else {
      UnionConfig(paramTool, null)
    }
  }
}
