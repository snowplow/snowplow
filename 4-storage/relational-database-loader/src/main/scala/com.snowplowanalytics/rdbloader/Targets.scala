package com.snowplowanalytics.rdbloader

import cats.syntax.either._
import io.circe.ParsingFailure
import org.json4s.JValue
import org.json4s.jackson.{parseJson => parseJson4s}

import scala.util.control.NonFatal

// Iglu client
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue._

import Utils._

object Targets {

  sealed trait SslMode extends StringEnum
  case object Disable extends SslMode { def asString = "DISABLE" }
  case object Require extends SslMode { def asString = "REQUIRE" }
  case object VerifyCa extends SslMode { def asString = "VERIFY_CA" }
  case object VerifyFull extends SslMode { def asString = "VERIFY_FULL" }

  sealed trait Purpose extends StringEnum
  case object DuplicateTracking extends Purpose { def asString = "DUPLICATE_TRACKING" }
  case object FailedEvents extends Purpose { def asString = "FAILED_EVENTS" }
  case object EnrichedEvents extends Purpose { def asString = "ENRICHED_EVENTS" }

  implicit val sslModeDecoder =
    decodeStringEnum[SslMode]

  implicit val purposeDecoder =
    decodeStringEnum[Purpose]

  sealed trait StorageTarget {
    def name: String
    def purpose: Purpose
  }

  def fooo(resolver: Resolver, target: String) = {
    val json = try {
      parseJson4s(target).asRight
    } catch {
      case NonFatal(e) => ParsingFailure("", e).asLeft
    }

    val data = json.toOption.get.validate(true)(resolver)
    data

  }

  case class ElasticConfig(
      name: String,
      host: String,
      index: String,
      port: Int,
      `type`: String,
      nodesWanOnly: Boolean)
    extends StorageTarget {
    val purpose = FailedEvents
  }

  case class AmazonDynamodbConfig(
      name: String,
      accessKeyId: String,
      secretAccessKey: String,
      awsRegions: String,
      dynamodbTable: String)
    extends StorageTarget {
    val purpose = DuplicateTracking
  }

  case class PostgresqlConfig(
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: String)
    extends StorageTarget {
    val purpose = EnrichedEvents
  }

  case class RedshiftConfig(
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: String,
      maxError: Int,
      compRows: Long)
    extends StorageTarget {
    val purpose = EnrichedEvents
  }
}
