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

import java.net.UnknownHostException

import scala.util.control.NonFatal

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._
import com.snowplowanalytics.forex.oerclient._
import com.snowplowanalytics.forex.{Forex, ForexConfig}
import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey}
import io.circe._
import org.joda.time.DateTime

import utils.CirceUtils

/** Companion object. Lets us create an CurrencyConversionEnrichment instance from a Json. */
object CurrencyConversionEnrichmentConfig extends ParseableEnrichment {

  val supportedSchema =
    SchemaCriterion(
      "com.snowplowanalytics.snowplow",
      "currency_conversion_config",
      "jsonschema",
      1,
      0)

  // Creates a CurrencyConversionEnrichment instance from a JValue
  def parse(
    c: Json,
    schemaKey: SchemaKey
  ): ValidatedNel[String, CurrencyConversionEnrichment] =
    isParseable(c, schemaKey)
      .leftMap(e => NonEmptyList.one(e))
      .flatMap { _ =>
        (
          CirceUtils.extract[String](c, "parameters", "apiKey").toValidatedNel,
          CirceUtils.extract[String](c, "parameters", "baseCurrency").toValidatedNel,
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
            .toValidatedNel,
          CirceUtils.extract[String](c, "parameters", "rateAt").toValidatedNel
        ).mapN { (apiKey, baseCurrency, accountType, rateAt) =>
            CurrencyConversionEnrichment(accountType, apiKey, baseCurrency, rateAt)
          }
          .toEither
      }
      .toValidated
}

/**
 * Configuration for a currency_conversion enrichment
 * @param apiKey OER authentication
 * @param baseCurrency Currency to which to convert
 * @param rateAt Which exchange rate to use - "EOD_PRIOR" for "end of previous day".
 */
final case class CurrencyConversionEnrichment(
  accountType: AccountType,
  apiKey: String,
  baseCurrency: String,
  rateAt: String
) extends Enrichment {
  val fx = Forex(ForexConfig(), OerClientConfig(apiKey, accountType))

  /**
   * Attempt to convert if the initial currency and value are both defined
   * @param inputCurrency Option boxing the initial currency if it is present
   * @param value Option boxing the amount to convert
   * @return None.success if the inputs were not both defined,
   * otherwise Validation[Option[_]] boxing the result of the conversion
   */
  private def performConversion(
    initialCurrency: Option[String],
    value: Option[Double],
    tstamp: DateTime
  ): Either[String, Option[String]] =
    (initialCurrency, value) match {
      case (Some(ic), Some(v)) =>
        fx.convert(v, ic)
          .to(baseCurrency)
          .at(tstamp)
          .bimap(
            l => {
              val errorType = l.errorType.getClass.getSimpleName.replace("$", "")
              s"Open Exchange Rates error, type: [$errorType], message: [${l.errorMessage}]"
            },
            r => (r.getAmount().toPlainString()).some
          )
      case _ => None.asRight
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
  ): ValidatedNel[String, (Option[String], Option[String], Option[String], Option[String])] =
    collectorTstamp match {
      case Some(tstamp) =>
        try {
          val newCurrencyTr = performConversion(trCurrency, trTotal, tstamp)
          val newCurrencyTi = performConversion(tiCurrency, tiPrice, tstamp)
          val newTrTax = performConversion(trCurrency, trTax, tstamp)
          val newTrShipping = performConversion(trCurrency, trShipping, tstamp)
          (
            newCurrencyTr.toValidatedNel,
            newTrTax.toValidatedNel,
            newTrShipping.toValidatedNel,
            newCurrencyTi.toValidatedNel
          ).mapN((_, _, _, _))
        } catch {
          case e: NoSuchElementException =>
            "Base currency [%s] not supported: [%s]".format(baseCurrency, e).invalidNel
          case f: UnknownHostException =>
            "Could not connect to Open Exchange Rates: [%s]".format(f).invalidNel
          case NonFatal(g) => "Unexpected exception converting currency: [%s]".format(g).invalidNel
        }
      case None => "Collector timestamp missing".invalidNel // This should never happen
    }
}
