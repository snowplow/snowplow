/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich.common
package enrichments.registry
package sqlquery

import scala.collection.immutable.IntMap

import cats.{Eval, Id, Monad}
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.lrumap._
import com.snowplowanalytics.snowplow.badrows._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import outputs.EnrichedEvent
import utils.CirceUtils

/** Lets us create an SqlQueryConf from a Json */
object SqlQueryEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "sql_query_enrichment_config",
      "jsonschema",
      1,
      0,
      0
    )

  /**
   * Creates an SqlQueryEnrichment instance from a Json.
   * @param c The enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @return a SqlQueryEnrichment configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, SqlQueryConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        // input ctor throws exception
        val inputs: ValidatedNel[String, List[Input]] = Either.catchNonFatal(
          CirceUtils.extract[List[Input]](c, "parameters", "inputs").toValidatedNel
        ) match {
          case Left(e) => e.getMessage.invalidNel
          case Right(r) => r
        }
        // output ctor throws exception
        val output: ValidatedNel[String, Output] = Either.catchNonFatal(
          CirceUtils.extract[Output](c, "parameters", "output").toValidatedNel
        ) match {
          case Left(e) => e.getMessage.invalidNel
          case Right(r) => r
        }
        (
          inputs,
          CirceUtils.extract[Db](c, "parameters", "database").toValidatedNel,
          CirceUtils.extract[Query](c, "parameters", "query").toValidatedNel,
          output,
          CirceUtils.extract[Cache](c, "parameters", "cache").toValidatedNel
        ).mapN { SqlQueryConf(schemaKey, _, _, _, _, _) }.toEither
      }
      .toValidated

  /**
   * Transform pairs of schema and node obtained from [[utils.shredder.Shredder]] into list of
   * regular self-describing JObject representing custom context or unstructured event.
   * If node isn't Self-describing (doesn't contain data key) it will be filtered out.
   * @param pairs list of pairs consisting of schema and Json nodes
   * @return list of regular JObjects
   */
  def transformRawPairs(pairs: List[SelfDescribingData[Json]]): List[Json] =
    pairs.map { p =>
      val uri = p.schema.toSchemaUri
      p.data.hcursor.downField("data").focus.map { json =>
        Json.obj(
          "schema" := Json.fromString(uri),
          "data" := json
        )
      }
    }.flatten

  def apply[F[_]: CreateSqlQueryEnrichment](conf: SqlQueryConf): F[SqlQueryEnrichment[F]] =
    CreateSqlQueryEnrichment[F].create(conf)
}

final case class SqlQueryEnrichment[F[_]: Monad: DbExecutor](
  schemaKey: SchemaKey,
  inputs: List[Input],
  db: Db,
  query: Query,
  output: Output,
  ttl: Int,
  cache: LruMap[F, IntMap[Input.ExtractedValue], (EitherThrowable[List[Json]], Long)]
) extends Enrichment {
  import SqlQueryEnrichment._

  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "sql-query").some

  /**
   * Primary function of the enrichment. Failure means connection failure, failed unexpected
   * JSON-value, etc. Successful Nil skipped lookup (unfilled placeholder for eg, empty response)
   * @param event currently enriching event
   * @param derivedContexts derived contexts as list of JSON objects
   * @param customContexts custom contexts as SelfDescribingData
   * @param unstructEvent unstructured (self-describing) event as empty or single element
   * SelfDescribingData
   * @return Nil if some inputs were missing, validated JSON contexts if lookup performed
   */
  def lookup(
    event: EnrichedEvent,
    derivedContexts: List[Json],
    customContexts: List[SelfDescribingData[Json]],
    unstructEvent: List[SelfDescribingData[Json]]
  ): F[ValidatedNel[FailureDetails.EnrichmentStageIssue, List[Json]]] = {
    val jsonCustomContexts = transformRawPairs(customContexts)
    val jsonUnstructEvent = transformRawPairs(unstructEvent).headOption

    val placeholderMap: Either[NonEmptyList[String], Input.PlaceholderMap] =
      Input
        .buildPlaceholderMap(inputs, event, derivedContexts, jsonCustomContexts, jsonUnstructEvent)
        .toEither
        .leftMap(_.map(_.getMessage))
        .flatMap(m => allPlaceholdersFilled(m).leftMap(NonEmptyList.one))

    placeholderMap match {
      case Right(Some(intMap)) =>
        EitherT(get(intMap))
          .leftMap(
            e =>
              FailureDetails.EnrichmentFailure(
                enrichmentInfo,
                FailureDetails.EnrichmentFailureMessage
                  .Simple(e.getMessage())
              )
          )
          .leftWiden
          .toValidatedNel
      case Right(None) => Monad[F].pure(Nil.validNel)
      case Left(es) =>
        val fs = es.map(
          e =>
            FailureDetails.EnrichmentFailure(
              enrichmentInfo,
              FailureDetails.EnrichmentFailureMessage.Simple(e)
            )
        )
        Monad[F].pure(fs.invalid)
    }
  }

  /**
   * Get contexts from cache or perform query if nothing found and put result into cache
   * @param intMap IntMap of extracted values
   * @return validated list of Self-describing contexts
   */
  def get(intMap: IntMap[Input.ExtractedValue]): F[EitherThrowable[List[Json]]] =
    for {
      gotten <- cache.get(intMap)
      res <- gotten match {
        case Some(response) =>
          if (System.currentTimeMillis() / 1000 - response._2 < ttl) Monad[F].pure(response._1)
          else put(intMap)
        case None => put(intMap)
      }
    } yield res

  private def put(intMap: IntMap[Input.ExtractedValue]): F[EitherThrowable[List[Json]]] =
    for {
      res <- query(intMap)
      _ <- cache.put(intMap, (res, System.currentTimeMillis() / 1000))
    } yield res

  /**
   * Perform SQL query and convert result to JSON object
   * @param intMap map with values extracted from inputs and ready to be set placeholders in
   * prepared statement
   * @return validated list of Self-describing contexts
   */
  def query(intMap: IntMap[Input.ExtractedValue]): F[EitherThrowable[List[Json]]] =
    (for {
      sqlQuery <- EitherT.fromEither[F](db.createStatement(query.sql, intMap))
      resultSet <- EitherT(db.execute[F](sqlQuery))
      context <- EitherT.fromEither[F](output.convert(resultSet))
    } yield context).value

  /**
   * Transform [[Input.PlaceholderMap]] to None if not enough input values were extracted
   * This prevents db from start building a statement while not failing event enrichment
   * @param placeholderMap some IntMap with extracted values or None if it is known already that not
   * all values were extracted
   * @return Some unchanged value if all placeholder were filled, None otherwise
   */
  private def allPlaceholdersFilled(
    placeholderMap: Input.PlaceholderMap
  ): Either[String, Input.PlaceholderMap] =
    getPlaceholderCount.map { placeholderCount =>
      placeholderMap match {
        case Some(intMap) if intMap.keys.size == placeholderCount => Some(intMap)
        case _ => None
      }
    }

  /** Stored amount of ?-signs in query.sql. Initialized once */
  private var lastPlaceholderCount: Either[Throwable, Int] =
    InvalidStateException("SQL Query Enrichment: placeholderCount hasn't been initialized").asLeft

  /**
   * If lastPlaceholderCount is successful return it
   * If it's unsucessfult - try to count save result for future use
   */
  def getPlaceholderCount: Either[String, Int] =
    lastPlaceholderCount
      .orElse {
        val newCount = db.getPlaceholderCount(query.sql)
        lastPlaceholderCount = newCount
        newCount.leftMap(_.toString)
      }
}

sealed trait CreateSqlQueryEnrichment[F[_]] {
  def create(conf: SqlQueryConf): F[SqlQueryEnrichment[F]]
}

object CreateSqlQueryEnrichment {
  def apply[F[_]](implicit ev: CreateSqlQueryEnrichment[F]): CreateSqlQueryEnrichment[F] = ev

  implicit def syncCreateSqlQueryEnrichment[F[_]: Sync: DbExecutor](
    implicit CLM: CreateLruMap[F, IntMap[Input.ExtractedValue], (EitherThrowable[List[Json]], Long)]
  ): CreateSqlQueryEnrichment[F] = new CreateSqlQueryEnrichment[F] {
    override def create(conf: SqlQueryConf): F[SqlQueryEnrichment[F]] =
      CLM
        .create(conf.cache.size)
        .map(
          c =>
            SqlQueryEnrichment(
              conf.schemaKey,
              conf.inputs,
              conf.db,
              conf.query,
              conf.output,
              conf.cache.ttl,
              c
            )
        )
  }

  implicit def evalCreateSqlQueryEnrichment(
    implicit CLM: CreateLruMap[Eval, IntMap[Input.ExtractedValue], (EitherThrowable[List[Json]], Long)],
    DB: DbExecutor[Eval]
  ): CreateSqlQueryEnrichment[Eval] = new CreateSqlQueryEnrichment[Eval] {
    override def create(conf: SqlQueryConf): Eval[SqlQueryEnrichment[Eval]] =
      CLM
        .create(conf.cache.size)
        .map(
          c =>
            SqlQueryEnrichment(
              conf.schemaKey,
              conf.inputs,
              conf.db,
              conf.query,
              conf.output,
              conf.cache.ttl,
              c
            )
        )
  }

  implicit def idCreateSqlQueryEnrichment(
    implicit CLM: CreateLruMap[Id, IntMap[Input.ExtractedValue], (EitherThrowable[List[Json]], Long)],
    DB: DbExecutor[Id]
  ): CreateSqlQueryEnrichment[Id] = new CreateSqlQueryEnrichment[Id] {
    override def create(conf: SqlQueryConf): Id[SqlQueryEnrichment[Id]] =
      CLM
        .create(conf.cache.size)
        .map(
          c =>
            SqlQueryEnrichment(
              conf.schemaKey,
              conf.inputs,
              conf.db,
              conf.query,
              conf.output,
              conf.cache.ttl,
              c
            )
        )
  }
}
