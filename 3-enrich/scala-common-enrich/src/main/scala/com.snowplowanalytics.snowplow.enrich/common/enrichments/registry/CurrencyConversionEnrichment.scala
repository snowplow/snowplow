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

import java.time.ZonedDateTime

import cats.Monad
import cats.data.{EitherT, NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.forex.{CreateForex, Forex}
import com.snowplowanalytics.forex.model._
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import com.snowplowanalytics.snowplow.badrows._
import io.circe._
import org.joda.money.CurrencyUnit
import org.joda.time.DateTime

import utils.CirceUtils

/** Companion object. Lets us create an CurrencyConversionEnrichment instance from a Json. */
object CurrencyConversionEnrichment extends ParseableEnrichment {
  override val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow",
      "currency_conversion_config",
      "jsonschema",
      1,
      0
    )

  // Creates a CurrencyConversionConf from a Json
  override def parse(
    c: Json,
    schemaKey: SchemaKey,
    localMode: Boolean = false
  ): ValidatedNel[String, CurrencyConversionConf] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          CirceUtils.extract[String](c, "parameters", "apiKey").toValidatedNel,
          CirceUtils
            .extract[String](c, "parameters", "baseCurrency")
            .toEither
            .flatMap(bc => Either.catchNonFatal(CurrencyUnit.of(bc)).leftMap(_.getMessage))
            .toValidatedNel,
          CirceUtils
            .extract[String](c, "parameters", "accountType")
            .toEither
            .flatMap {
              case "DEVELOPER" => DeveloperAccount.asRight
              case "ENTERPRISE" => EnterpriseAccount.asRight
              case "UNLIMITED" => UnlimitedAccount.asRight
              // Should never happen (prevented by schema validation)
              case s =>
                s"accountType [$s] is not one of DEVELOPER, ENTERPRISE, and UNLIMITED".asLeft
            }
            .toValidatedNel
        ).mapN { (apiKey, baseCurrency, accountType) =>
          CurrencyConversionConf(schemaKey, accountType, apiKey, baseCurrency)
        }.toEither
      }
      .toValidated

  /**
   * Creates a CurrencyConversionEnrichment from a CurrencyConversionConf
   * @param conf Configuration for the currency conversion enrichment
   * @return a currency conversion enrichment
   */
  def apply[F[_]: Monad: CreateForex](
    conf: CurrencyConversionConf
  ): F[CurrencyConversionEnrichment[F]] =
    CreateForex[F]
      .create(
        ForexConfig(conf.apiKey, conf.accountType, baseCurrency = conf.baseCurrency)
      )
      .map(f => CurrencyConversionEnrichment(conf.schemaKey, f, conf.baseCurrency))
}

/**
 * Currency conversion enrichment
 * @param forex Forex client
 * @param baseCurrency the base currency to refer to
 */
final case class CurrencyConversionEnrichment[F[_]: Monad](
  schemaKey: SchemaKey,
  forex: Forex[F],
  baseCurrency: CurrencyUnit
) extends Enrichment {
  private val enrichmentInfo =
    FailureDetails.EnrichmentInformation(schemaKey, "currency-conversion").some

  /**
   * Attempt to convert if the initial currency and value are both defined
   * @param inputCurrency Option boxing the initial currency if it is present
   * @param value Option boxing the amount to convert
   * @return None.success if the inputs were not both defined,
   * otherwise Validation[Option[_]] boxing the result of the conversion
   */
  private def performConversion(
    initialCurrency: Option[Either[FailureDetails.EnrichmentStageIssue, CurrencyUnit]],
    value: Option[Double],
    tstamp: ZonedDateTime
  ): F[Either[FailureDetails.EnrichmentStageIssue, Option[String]]] =
    (initialCurrency, value) match {
      case (Some(ic), Some(v)) =>
        (for {
          cu <- EitherT.fromEither[F](ic)
          res <- EitherT(
            forex
              .convert(v, cu)
              .to(baseCurrency)
              .at(tstamp)
              .map(
                _.bimap(
                  l => {
                    val errorType = l.errorType.getClass.getSimpleName.replace("$", "")
                    val msg =
                      s"Open Exchange Rates error, type: [$errorType], message: [${l.errorMessage}]"
                    val f =
                      FailureDetails.EnrichmentFailureMessage.Simple(msg)
                    FailureDetails
                      .EnrichmentFailure(enrichmentInfo, f): FailureDetails.EnrichmentStageIssue
                  },
                  r => (r.getAmount().toPlainString()).some
                )
              )
          )
        } yield res).value
      case _ => Monad[F].pure(None.asRight)
    }

  /**
   * Converts currency using Scala Forex
   * @param trCurrency Initial transaction currency
   * @param trTotal Total transaction value
   * @param trTax Transaction tax
   * @param trShipping Transaction shipping cost
   * @param tiCurrency Initial transaction item currency
   * @param tiPrice Initial transaction item price
   * @param collectorTstamp Collector timestamp
   * @return Validation[Tuple] containing all input amounts converted to the base currency
   */
  def convertCurrencies(
    trCurrency: Option[String],
    trTotal: Option[Double],
    trTax: Option[Double],
    trShipping: Option[Double],
    tiCurrency: Option[String],
    tiPrice: Option[Double],
    collectorTstamp: Option[DateTime]
  ): F[ValidatedNel[
    FailureDetails.EnrichmentStageIssue,
    (Option[String], Option[String], Option[String], Option[String])
  ]] =
    collectorTstamp match {
      case Some(tstamp) =>
        val zdt = tstamp.toGregorianCalendar().toZonedDateTime()
        val trCu = trCurrency.map { c =>
          Either
            .catchNonFatal(CurrencyUnit.of(c))
            .leftMap { e =>
              val f = FailureDetails.EnrichmentFailureMessage.InputData(
                "tr_currency",
                trCurrency,
                e.getMessage
              )
              FailureDetails.EnrichmentFailure(enrichmentInfo, f)
            }
        }
        val tiCu = tiCurrency.map { c =>
          Either
            .catchNonFatal(CurrencyUnit.of(c))
            .leftMap { e =>
              val f = FailureDetails.EnrichmentFailureMessage.InputData(
                "ti_currency",
                tiCurrency,
                e.getMessage
              )
              FailureDetails.EnrichmentFailure(enrichmentInfo, f)
            }
        }
        (
          performConversion(trCu, trTotal, zdt),
          performConversion(trCu, trTax, zdt),
          performConversion(trCu, trShipping, zdt),
          performConversion(tiCu, tiPrice, zdt)
        ).mapN(
          (newCurrencyTr, newTrTax, newTrShipping, newCurrencyTi) =>
            (
              newCurrencyTr.toValidatedNel,
              newTrTax.toValidatedNel,
              newTrShipping.toValidatedNel,
              newCurrencyTi.toValidatedNel
            ).mapN((_, _, _, _))
        )
      // This should never happen
      case None =>
        val f = FailureDetails.EnrichmentFailureMessage.InputData(
          "collector_tstamp",
          None,
          "missing"
        )
        Monad[F].pure(FailureDetails.EnrichmentFailure(enrichmentInfo, f).invalidNel)
    }
}
