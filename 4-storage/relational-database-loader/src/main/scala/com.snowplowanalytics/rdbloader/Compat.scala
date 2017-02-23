package com.snowplowanalytics.rdbloader

import io.circe.Json
import org.json4s.JsonAST._

object Compat {
  implicit def jvalueToCirce(jValue: JValue): Json = jValue match {
    case JString(s)    => Json.fromString(s)
    case JObject(vals) => Json.fromFields(vals.map { case (k, v) => (k, jvalueToCirce(v))})
    case JInt(i)       => Json.fromBigInt(i)
    case JDouble(d)    => Json.fromDoubleOrNull(d)
    case JBool(b)      => Json.fromBoolean(b)
    case JArray(arr)   => Json.arr(arr.map(jvalueToCirce): _*)
    case JNull         => Json.Null
    case JDecimal(d)   => Json.fromBigDecimal(d)
    case JNothing      => Json.Null
  }
}
