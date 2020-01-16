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
package enrichments.registry

import java.io.File
import java.net.{InetAddress, URI}

import cats.{Eval, Id, Monad}
import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.Sync
import cats.implicits._

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, SchemaVer, SelfDescribingData}

import com.snowplowanalytics.iab.spidersandrobotsclient.IabClient
import com.snowplowanalytics.snowplow.badrows.FailureDetails

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

import org.joda.time.DateTime

import utils.CirceUtils

/** Companion object. Lets us create an IabEnrichment instance from a Json. */
object IabEnrichment extends ParseableEnrichment {
  override val supportedSchema = SchemaCriterion(
    "com.snowplowanalytics.snowplow.enrichments",
    "iab_spiders_and_robots_enrichment",
    "jsonschema",
    1,
    0
  )

  /**
   * Creates an IabConf from a Json.
   * @param c The iab_spiders_and_robots_enrichment JSON
   * @param schemaKey provided for the enrichment, must be supported by this enrichment
   * @param localMode Whether to use the local IAB database file, enabled for tests
   * @return a Iab configuration
   */
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean
  ): ValidatedNel[String, IabConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          getIabDbFromName(c, "ipFile"),
          getIabDbFromName(c, "excludeUseragentFile"),
          getIabDbFromName(c, "includeUseragentFile")
        ).mapN { (ip, exclude, include) =>
          IabConf(
            schemaKey,
            file(ip, localMode),
            file(exclude, localMode),
            file(include, localMode)
          )
        }.toEither
      }
      .toValidated

  private def file(db: IabDatabase, localMode: Boolean): (URI, String) =
    if (localMode) (db.uri, getClass.getResource(db.db).toURI.getPath)
    else (db.uri, s"./iab_${db.name}")

  /**
   * Creates an IabEnrichment from a IabConf
   * @param conf Configuration for the iab enrichment
   * @return an iab enrichment
   */
  def apply[F[_]: Monad: CreateIabClient](conf: IabConf): F[IabEnrichment] =
    CreateIabClient[F]
      .create(conf.ipFile._2, conf.excludeUaFile._2, conf.includeUaFile._2)
      .map(c => IabEnrichment(conf.schemaKey, c))

  /**
   * Creates IabDatabase instances used in the IabEnrichment case class.
   * @param config The iab_spiders_and_robots_enrichment JSON
   * @param name of the field, e.g. "ipFile", "excluseUseragentFile", "includeUseragentFile"
   * @return None if the field does not exist, Some(Failure) if the URI is invalid, Some(Success) if
   * it is found
   */
  private def getIabDbFromName(config: Json, name: String): ValidatedNel[String, IabDatabase] = {
    val uri = CirceUtils.extract[String](config, "parameters", name, "uri")
    val db = CirceUtils.extract[String](config, "parameters", name, "database")

    // better-monadic-for
    (for {
      uriAndDb <- (uri.toValidatedNel, db.toValidatedNel).mapN { (_, _) }.toEither
      uri <- getDatabaseUri(uriAndDb._1, uriAndDb._2).leftMap(NonEmptyList.one)
    } yield IabDatabase(name, uri, uriAndDb._2)).toValidated
  }
}

/**
 * Contains enrichments based on IAB Spiders&Robots lookup.
 * @param ipFile (Full URI to the IAB excluded IP list, database name)
 * @param excludeUaFile (Full URI to the IAB excluded user agent list, database name)
 * @param includeUaFile (Full URI to the IAB included user agent list, database name)
 * @param localMode Whether to use the local database file. Enabled for tests.
 */
final case class IabEnrichment(schemaKey: SchemaKey, iabClient: IabClient) extends Enrichment {
  val outputSchema =
    SchemaKey("com.iab.snowplow", "spiders_and_robots", "jsonschema", SchemaVer.Full(1, 0, 0))
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "iab-spiders-and-robots").some

  /**
   * Get the IAB response containing information about whether an event is a spider or robot using
   * the IAB client library.
   * @param userAgent User agent used to perform the check
   * @param ipAddress IP address used to perform the check
   * @param accurateAt Date of the event, used to determine whether entries in the IAB list are
   * relevant or outdated
   * @return an IabResponse object
   */
  private[enrichments] def performCheck(
    userAgent: String,
    ipAddress: String,
    accurateAt: DateTime
  ): Either[FailureDetails.EnrichmentStageIssue, IabEnrichmentResponse] =
    (for {
      ip <- Either
        .catchNonFatal(InetAddress.getByName(ipAddress))
        .leftMap(
          e =>
            FailureDetails.EnrichmentFailureMessage
              .InputData("user_ipaddress", ipAddress.some, e.getMessage)
        )
      result <- Either
        .catchNonFatal(iabClient.checkAt(userAgent, ip, accurateAt.toDate))
        .leftMap(
          e => FailureDetails.EnrichmentFailureMessage.Simple(e.getMessage)
        )
    } yield IabEnrichmentResponse(
      result.isSpiderOrRobot,
      result.getCategory.toString,
      result.getReason.toString,
      result.getPrimaryImpact.toString
    )).leftMap(FailureDetails.EnrichmentFailure(enrichmentInfo, _))

  /**
   * Get the IAB response as a JSON context for a specific event
   * @param userAgent enriched event optional user agent
   * @param ipAddress enriched event optional IP address
   * @param accurateAt enriched event optional datetime
   * @return IAB response as a self-describing JSON object
   */
  def getIabContext(
    userAgent: Option[String],
    ipAddress: Option[String],
    accurateAt: Option[DateTime]
  ): Either[NonEmptyList[FailureDetails.EnrichmentStageIssue], SelfDescribingData[Json]] =
    getIab(userAgent, ipAddress, accurateAt).map { iab =>
      SelfDescribingData(outputSchema, iab)
    }

  /**
   * Get IAB check response received from the client library and extracted as a JSON object
   * @param userAgent enriched event optional user agent
   * @param ipAddress enriched event optional IP address
   * @param time enriched event optional datetime
   * @return IAB response as JSON object
   */
  private def getIab(
    userAgent: Option[String],
    ipAddress: Option[String],
    time: Option[DateTime]
  ): Either[NonEmptyList[FailureDetails.EnrichmentStageIssue], Json] =
    (userAgent, ipAddress, time) match {
      case (Some(ua), Some(ip), Some(t)) =>
        performCheck(ua, ip, t)
          .map(_.asJson)
          .leftMap(NonEmptyList.one)
      case (a, b, c) =>
        val failures = List((a, "useragent"), (b, "user_ipaddress"), (c, "derived_tstamp"))
          .collect {
            case (None, n) =>
              val f = FailureDetails.EnrichmentFailureMessage.InputData(
                n,
                none,
                "missing"
              )
              FailureDetails.EnrichmentFailure(enrichmentInfo, f)
          }
        NonEmptyList
          .fromList(failures)
          .getOrElse(NonEmptyList.of {
            val f = FailureDetails.EnrichmentFailureMessage
              .Simple("could not construct failures")
            FailureDetails.EnrichmentFailure(enrichmentInfo, f)
          })
          .asLeft
    }
}

trait CreateIabClient[F[_]] {
  def create(
    ipFile: String,
    excludeUaFile: String,
    includeUaFile: String
  ): F[IabClient]
}

object CreateIabClient {
  def apply[F[_]](implicit ev: CreateIabClient[F]): CreateIabClient[F] = ev

  implicit def syncCreateIabClient[F[_]: Sync]: CreateIabClient[F] = new CreateIabClient[F] {
    def create(
      ipFile: String,
      excludeUaFile: String,
      includeUaFile: String
    ): F[IabClient] =
      Sync[F].delay {
        new IabClient(new File(ipFile), new File(excludeUaFile), new File(includeUaFile))
      }
  }

  implicit def evalCreateIabClient: CreateIabClient[Eval] = new CreateIabClient[Eval] {
    def create(
      ipFile: String,
      excludeUaFile: String,
      includeUaFile: String
    ): Eval[IabClient] =
      Eval.later {
        new IabClient(new File(ipFile), new File(excludeUaFile), new File(includeUaFile))
      }
  }

  implicit def idCreateIabClient: CreateIabClient[Id] = new CreateIabClient[Id] {
    def create(
      ipFile: String,
      excludeUaFile: String,
      includeUaFile: String
    ): Id[IabClient] =
      new IabClient(new File(ipFile), new File(excludeUaFile), new File(includeUaFile))
  }
}

/** Case class copy of `com.snowplowanalytics.iab.spidersandrobotsclient.IabResponse` */
private[enrichments] final case class IabEnrichmentResponse(
  spiderOrRobot: Boolean,
  category: String,
  reason: String,
  primaryImpact: String
)

/** Case class representing an IAB database location */
private[enrichments] final case class IabDatabase(
  name: String,
  uri: URI,
  db: String
)
