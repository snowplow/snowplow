package com.snowplowanalytics.snowplow.enrich.common
package loaders

import scalaz._
import Scalaz._

import java.util.UUID

import scala.concurrent.ExecutionContext.global
import com.snowplowanalytics.snowplow.enrich.common.ValidatedMaybeCollectorPayload

import org.apache.http.message.BasicNameValuePair
import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.json4s.JsonAST.{JObject, JString}

import scala.util.control.NonFatal

object PubSubLoader extends Loader[(Array[Byte], Map[String, String])] {
  def toCollectorPayload(line: (Array[Byte], Map[String, String])): ValidatedMaybeCollectorPayload = {
    val (data, attributes) = line
    val json = try {
      JsonMethods.parse(new String(data)).successNel
    } catch {
      case NonFatal(e) => s"Error parsing JSON from payload  ${e.getMessage}".failureNel
    }

    val timestamp    = DateTime.now()
    val collectorApi = CollectorApi("com.snowplowanalytics.snowplow", "tp2")
    val source       = CollectorSource("com.google", "pubsub", None)
    val context      = CollectorContext(Some(timestamp), None, None, None, Nil, None)

    for {
      j  <- json
      kv <- fromPayload(j)
      pairs = kv.map { case (k, v) => new BasicNameValuePair(k, v) }
    } yield Some(CollectorPayload(collectorApi, pairs, None, None, source, context))
  }

  def fromPayload(json: JValue): Validated[List[(String, String)]] = json match {
    case JObject(fields) =>
      fields.traverse {
        case (key, JString(value)) =>
          (key, value).successNel[String]
        case (_, value) =>
          s"Value $value in payload is not a string".failureNel[(String, String)]
      }
    case other =>
      s"Payload $other expected to be a JSON object".failureNel[List[(String, String)]]
  }
}
