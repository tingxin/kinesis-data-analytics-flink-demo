package cn.nwcd.presales.common.util

import org.json4s.JsonAST.{JNothing, JNull, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse}
import org.json4s.jackson.Serialization
import org.json4s.{DefaultFormats, Extraction}

import scala.util.Try

object JsonUtils {
  implicit val formats = DefaultFormats

  def toJson(objectToWrite: AnyRef): String = Serialization.write(objectToWrite)

  def fromJsonOption[T](jsonString: String)(implicit mf: Manifest[T]): Option[T] = Try(Serialization.read[T](jsonString)).toOption

  def fromJson[T](jsonString: String)(implicit mf: Manifest[T]): T = Serialization.read[T](jsonString)

  def jsonStrToMap(jsonStr: String): Map[String, Any] = parse(jsonStr).extract[Map[String, Any]]

  def toMap(obj: AnyRef): Map[String, Any] = jsonStrToMap(toJson(obj))

  def encodeJson(obj: AnyRef): JValue = Extraction.decompose(obj)

  def decodeJson[T](obj: JValue)(implicit mf: Manifest[T]): Option[T] = Try(Extraction.extract[T](obj)).toOption

  def convertToJValue(jsonStr: String) = parse(jsonStr)

  def convertToJString(jValue: JValue) = compact(jValue)

  def extractValue[T](jsonStr: String, key: String)(implicit mf: Manifest[T]): Option[T] = extractValue[T](parse(jsonStr), key)

  def extractValue[T](json: JValue, key: String)(implicit mf: Manifest[T]): Option[T] = {
    val value = json \ key
    value match {
      case JNothing => None
      case JNull => None
      case _ => value.extractOpt[T]
    }
  }
}
