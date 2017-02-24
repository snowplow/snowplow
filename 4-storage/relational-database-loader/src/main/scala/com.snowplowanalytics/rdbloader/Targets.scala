package com.snowplowanalytics.rdbloader

import scala.util.control.NonFatal

import cats.syntax.either._

import io.circe.{Json, ParsingFailure}
import io.circe.Decoder._
import io.circe.generic.auto._

import org.json4s.JValue
import org.json4s.jackson.{parseJson => parseJson4s}

// Iglu client
import com.snowplowanalytics.iglu.core.SchemaKey
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

  sealed trait StorageTarget extends Product with Serializable {
    def name: String
    def purpose: Purpose
  }

  def safeParse(target: String): Either[ConfigError, JValue] =
    try {
      parseJson4s(target).asRight
    } catch {
      case NonFatal(e) => ParseError(ParsingFailure("Invalid storage target JSON", e)).asLeft
    }

  def validate(resolver: Resolver, json: JValue): Either[ValidationError, Json] = {
    json.validate(dataOnly = false)(resolver).leftMap(l => ValidationError(l.list)).toEither.map(Compat.jvalueToCirce)
  }

  def fooo(resolver: Resolver)(target: String): Either[ConfigError, StorageTarget] = {
    for {
      j <- safeParse(target)
      p <- validate(resolver, j)
      s <- decodeStorageTarget(p)
    } yield s
  }

  def decodeStorageTarget(validJson: Json): Either[DecodingError, StorageTarget] = {
    val nameDataPair = for {
      jsonObject <- validJson.asObject
      schema <- jsonObject.toMap.get("schema")
      data <- jsonObject.toMap.get("data")
      schemaKey <- schema.asString
      key <- SchemaKey.fromUri(schemaKey)
    } yield (key.name, data)

    nameDataPair match {
      case Some(("elastic_config", data)) => data.as[ElasticConfig].leftMap(DecodingError.apply)
      case Some(("amazon_dynamodb_config", data)) => data.as[AmazonDynamodbConfig].leftMap(DecodingError.apply)
      case Some(("postgresql_config", data)) => data.as[PostgresqlConfig].leftMap(DecodingError.apply)
      case Some(("redshift_config", data)) => data.as[RedshiftConfig].leftMap(DecodingError.apply)
      case Some((name, _)) => DecodingError(s"Unknown storage target [$name]").asLeft
      case None => DecodingError("Not a self-describing JSON was used as storage target configuration").asLeft
    }
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
      awsRegion: String,
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
