/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package enrichments
package registry
package apirequest

import java.util.UUID

import cats.{Eval, Id, Monad}
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.effect.Sync
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.lrumap._

import com.snowplowanalytics.snowplow.badrows.FailureDetails

import io.circe._
import io.circe.generic.auto._

import outputs.EnrichedEvent
import utils.{CirceUtils, HttpClient}

object ApiRequestEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow.enrichments",
      "api_request_enrichment_config",
      "jsonschema",
      1,
      0,
      0
    )

  /**
   * Creates an ApiRequestEnrichment instance from a JValue.
   * @param c The enrichment JSON (not self-describing)
   * @param schemaKey The SchemaKey provided for the enrichment
   * Must be a supported SchemaKey for this enrichment
   * @return a configured ApiRequestEnrichment instance
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, ApiRequestConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        // input ctor throws exception
        val inputs: ValidatedNel[String, List[Input]] = Either.catchNonFatal {
          CirceUtils.extract[List[Input]](c, "parameters", "inputs").toValidatedNel
        } match {
          case Left(e) => e.getMessage.invalidNel
          case Right(r) => r
        }
        (
          inputs,
          CirceUtils.extract[HttpApi](c, "parameters", "api", "http").toValidatedNel,
          CirceUtils.extract[List[Output]](c, "parameters", "outputs").toValidatedNel,
          CirceUtils.extract[Cache](c, "parameters", "cache").toValidatedNel
        ).mapN { (inputs, api, outputs, cache) =>
          ApiRequestConf(schemaKey, inputs, api, outputs, cache)
        }.toEither
      }
      .toValidated

  /**
   * Creates an UUID based on url and optional body.
   * @param url URL to query
   * @param body optional request body
   * @return UUID that identifies of the request.
   */
  def cacheKey(url: String, body: Option[String]): String = {
    val contentKey = url + body.getOrElse("")
    UUID.nameUUIDFromBytes(contentKey.getBytes).toString
  }

  def apply[F[_]: CreateApiRequestEnrichment](conf: ApiRequestConf): F[ApiRequestEnrichment[F]] =
    CreateApiRequestEnrichment[F].create(conf)
}

final case class ApiRequestEnrichment[F[_]: Monad: HttpClient](
  schemaKey: SchemaKey,
  inputs: List[Input],
  api: HttpApi,
  outputs: List[Output],
  ttl: Int,
  cache: LruMap[F, String, (Either[Throwable, Json], Long)]
) extends Enrichment {
  import ApiRequestEnrichment._

  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "api-request").some

  /**
   * Primary function of the enrichment. Failure means HTTP failure, failed unexpected JSON-value,
   * etc. Successful None skipped lookup (missing key for eg.)
   * @param event currently enriching event
   * @param derivedContexts derived contexts
   * @return none if some inputs were missing, validated JSON context if lookup performed
   */
  def lookup(
    event: EnrichedEvent,
    derivedContexts: List[SelfDescribingData[Json]],
    customContexts: List[SelfDescribingData[Json]],
    unstructEvent: Option[SelfDescribingData[Json]]
  ): F[ValidatedNel[FailureDetails.EnrichmentFailure, List[SelfDescribingData[Json]]]] = {
    val templateContext =
      Input.buildTemplateContext(
        inputs,
        event,
        derivedContexts,
        customContexts,
        unstructEvent
      )

    val contexts: EitherT[F, NonEmptyList[String], List[SelfDescribingData[Json]]] =
      for {
        context <- EitherT.fromEither[F](templateContext.toEither)
        jsons <- getOutputs(context)
        contexts = jsons.parTraverse { json =>
          SelfDescribingData
            .parse(json)
            .leftMap(e => NonEmptyList.one(s"${json.noSpaces} is not self-describing, ${e.code}"))
        }
        outputs <- EitherT.fromEither[F](contexts)
      } yield outputs

    contexts.leftMap(failureDetails).toValidated
  }

  /**
   * Build URI and try to get value for each of [[outputs]]
   * @param validInputs map to build template context
   * @return validated list of lookups, whole lookup will be failed if any of outputs were failed
   */
  private[apirequest] def getOutputs(validInputs: Option[Map[String, String]]): EitherT[F, NonEmptyList[String], List[Json]] = {
    import cats.instances.parallel._
    val result: List[F[Either[Throwable, Json]]] =
      for {
        templateContext <- validInputs.toList
        url <- api.buildUrl(templateContext).toList
        output <- outputs
        body = api.buildBody(templateContext)
      } yield cachedOrRequest(url, body, output)

    result.parTraverse { action =>
      EitherT(action.map { result =>
        result.leftMap(_.getMessage).toEitherNel
      })
    }
  }

  /**
   * Check cache for URL and perform HTTP request if value wasn't found
   * @param url URL to request
   * @param output currently processing output
   * @return validated JObject, in case of success ready to be attached to derived contexts
   */
  private[apirequest] def cachedOrRequest(
    url: String,
    body: Option[String],
    output: Output
  ): F[Either[Throwable, Json]] =
    for {
      key <- Monad[F].pure(cacheKey(url, body))
      gotten <- cache.get(key)
      res <- gotten match {
        case Some(response) =>
          if (System.currentTimeMillis() / 1000 - response._2 < ttl) Monad[F].pure(response._1)
          else put(key, url, body, output)
        case None => put(key, url, body, output)
      }
      extracted = res.flatMap(output.extract)
      described = extracted.map(output.describeJson)
    } yield described

  private def put(
    key: String,
    url: String,
    body: Option[String],
    output: Output
  ): F[Either[Throwable, Json]] =
    for {
      response <- api.perform[F](url, body)
      json = response.flatMap(output.parseResponse)
      _ <- cache.put(key, (json, System.currentTimeMillis() / 1000))
    } yield json

  private def failureDetails(errors: NonEmptyList[String]) =
    errors.map { error =>
      val message = FailureDetails.EnrichmentFailureMessage.Simple(error)
      FailureDetails.EnrichmentFailure(enrichmentInfo, message)
    }
}

sealed trait CreateApiRequestEnrichment[F[_]] {
  def create(conf: ApiRequestConf): F[ApiRequestEnrichment[F]]
}

object CreateApiRequestEnrichment {
  def apply[F[_]](implicit ev: CreateApiRequestEnrichment[F]): CreateApiRequestEnrichment[F] = ev

  implicit def syncCreateApiRequestEnrichment[F[_]: Sync: HttpClient](
    implicit CLM: CreateLruMap[F, String, (Either[Throwable, Json], Long)]
  ): CreateApiRequestEnrichment[F] = new CreateApiRequestEnrichment[F] {
    override def create(conf: ApiRequestConf): F[ApiRequestEnrichment[F]] =
      CLM
        .create(conf.cache.size)
        .map(
          c =>
            ApiRequestEnrichment(
              conf.schemaKey,
              conf.inputs,
              conf.api,
              conf.outputs,
              conf.cache.ttl,
              c
            )
        )
  }

  implicit def evalCreateApiRequestEnrichment(
    implicit CLM: CreateLruMap[Eval, String, (Either[Throwable, Json], Long)],
    HTTP: HttpClient[Eval]
  ): CreateApiRequestEnrichment[Eval] = new CreateApiRequestEnrichment[Eval] {
    override def create(conf: ApiRequestConf): Eval[ApiRequestEnrichment[Eval]] =
      CLM
        .create(conf.cache.size)
        .map(
          c =>
            ApiRequestEnrichment(
              conf.schemaKey,
              conf.inputs,
              conf.api,
              conf.outputs,
              conf.cache.ttl,
              c
            )
        )
  }

  implicit def idCreateApiRequestEnrichment(
    implicit CLM: CreateLruMap[Id, String, (Either[Throwable, Json], Long)],
    HTTP: HttpClient[Id]
  ): CreateApiRequestEnrichment[Id] = new CreateApiRequestEnrichment[Id] {
    override def create(conf: ApiRequestConf): Id[ApiRequestEnrichment[Id]] =
      CLM
        .create(conf.cache.size)
        .map(
          c =>
            ApiRequestEnrichment(
              conf.schemaKey,
              conf.inputs,
              conf.api,
              conf.outputs,
              conf.cache.ttl,
              c
            )
        )
  }
}
