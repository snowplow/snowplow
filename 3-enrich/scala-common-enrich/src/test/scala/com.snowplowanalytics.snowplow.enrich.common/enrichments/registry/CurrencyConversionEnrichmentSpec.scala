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

import cats.Eval
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.option._
import cats.syntax.validated._
import com.snowplowanalytics.forex.CreateForex._
import com.snowplowanalytics.forex.model._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows._
import org.joda.money.CurrencyUnit
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.DataTables

object CurrencyConversionEnrichmentSpec {
  val OerApiKey = "OER_KEY"
}

/** Tests the convertCurrencies function */
import CurrencyConversionEnrichmentSpec._
class CurrencyConversionEnrichmentSpec extends Specification with DataTables {
  def is =
    skipAllIf(sys.env.get(OerApiKey).isEmpty) ^
      s2"""
  Failure test for Currency Conversion $e1
  Success test for Currency Conversion $e2
  """

  lazy val validAppKey = sys.env
    .get(OerApiKey)
    .getOrElse(
      throw new IllegalStateException(
        s"No ${OerApiKey} environment variable found, test should have been skipped"
      )
    )
  type Result = ValidatedNel[
    FailureDetails.EnrichmentStageIssue,
    (Option[String], Option[String], Option[String], Option[String])
  ]
  val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))
  val ef: FailureDetails.EnrichmentFailureMessage => FailureDetails.EnrichmentFailure = m =>
    FailureDetails.EnrichmentFailure(
      FailureDetails.EnrichmentInformation(schemaKey, "currency-conversion").some,
      m
    )
  val currencyInvalidRup: Result = Validated.Invalid(
    NonEmptyList.of(
      ef(
        FailureDetails.EnrichmentFailureMessage
          .InputData("tr_currency", Some("RUP"), "Unknown currency 'RUP'")
      ),
      ef(
        FailureDetails.EnrichmentFailureMessage
          .InputData("tr_currency", Some("RUP"), "Unknown currency 'RUP'")
      ),
      ef(
        FailureDetails.EnrichmentFailureMessage
          .InputData("tr_currency", Some("RUP"), "Unknown currency 'RUP'")
      )
    )
  )
  val currencyInvalidHul: Result = ef(
    FailureDetails.EnrichmentFailureMessage.InputData(
      "ti_currency",
      Some("HUL"),
      "Unknown currency 'HUL'"
    )
  ).invalidNel
  val invalidAppKeyFailure: Result = ef(
    FailureDetails.EnrichmentFailureMessage.Simple(
      "Open Exchange Rates error, type: [OtherErrors], message: [invalid_app_id]"
    )
  ).invalidNel
  val coTstamp: DateTime = new DateTime(2011, 3, 13, 0, 0)

  def e1 =
    "SPEC NAME" || "TRANSACTION CURRENCY" | "API KEY" | "TOTAL AMOUNT" | "TOTAL TAX" | "SHIPPING" | "TRANSACTION ITEM CURRENCY" | "TRANSACTION ITEM PRICE" | "DATETIME" | "CONVERTED TUPLE" |
      "Invalid transaction currency" !! Some("RUP") ! validAppKey ! Some(11.00) ! Some(1.17) ! Some(
        0.00
      ) ! None ! Some(17.99) ! Some(coTstamp) ! currencyInvalidRup |
      "Invalid transaction item currency" !! None ! validAppKey ! Some(12.00) ! Some(0.7) ! Some(
        0.00
      ) ! Some("HUL") ! Some(1.99) ! Some(coTstamp) ! currencyInvalidHul |
      "Invalid OER API key" !! None ! "8A8A8A8A8A8A8A8A8A8A8A8AA8A8A8A8" ! Some(13.00) ! Some(3.67) ! Some(
        0.00
      ) ! Some("GBP") ! Some(2.99) ! Some(coTstamp) ! invalidAppKeyFailure |> {
      (
        _,
        trCurrency,
        apiKey,
        trAmountTotal,
        trAmountTax,
        trAmountShipping,
        tiCurrency,
        tiPrice,
        dateTime,
        expected
      ) =>
        (for {
          e <- CurrencyConversionConf(schemaKey, DeveloperAccount, apiKey, CurrencyUnit.EUR)
            .enrichment[Eval]
          res <- e.convertCurrencies(
            trCurrency,
            trAmountTotal,
            trAmountTax,
            trAmountShipping,
            tiCurrency,
            tiPrice,
            dateTime
          )
        } yield res).value must_== expected
    }

  def e2 =
    "SPEC NAME" || "TRANSACTION CURRENCY" | "API KEY" | "TOTAL AMOUNT" | "TOTAL TAX" | "SHIPPING" | "TRANSACTION ITEM CURRENCY" | "TRANSACTION ITEM PRICE" | "DATETIME" | "CONVERTED TUPLE" |
      "All fields absent" !! None ! validAppKey ! None ! None ! None ! None ! None ! None ! ef(
        FailureDetails.EnrichmentFailureMessage.InputData(
          "collector_tstamp",
          None,
          "missing"
        )
      ).invalidNel |
      "All fields absent except currency" !! Some("GBP") ! validAppKey ! None ! None ! None ! Some(
        "GBP"
      ) ! None ! None ! ef(
        FailureDetails.EnrichmentFailureMessage
          .InputData("collector_tstamp", None, "missing")
      ).invalidNel |
      "No transaction currency, tax, or shipping" !! Some("GBP") ! validAppKey ! Some(11.00) ! None ! None ! None ! None ! Some(
        coTstamp
      ) ! (Some("12.75"), None, None, None).valid |
      "No transaction currency or total" !! Some("GBP") ! validAppKey ! None ! Some(2.67) ! Some(
        0.00
      ) ! None ! None ! Some(coTstamp) ! (None, Some("3.09"), Some("0.00"), None).valid |
      "No transaction currency" !! None ! validAppKey ! None ! None ! None ! Some("GBP") ! Some(
        12.99
      ) ! Some(coTstamp) ! (None, None, None, Some("15.05")).valid |
      "Transaction Item Null" !! Some("GBP") ! validAppKey ! Some(11.00) ! Some(2.67) ! Some(0.00) ! None ! None ! Some(
        coTstamp
      ) ! (Some("12.75"), Some("3.09"), Some("0.00"), None).valid |
      "Valid APP ID and API key" !! None ! validAppKey ! Some(14.00) ! Some(4.67) ! Some(0.00) ! Some(
        "GBP"
      ) ! Some(10.99) ! Some(coTstamp) ! (None, None, None, Some("12.74")).valid |
      "Both Currency Null" !! None ! validAppKey ! Some(11.00) ! Some(2.67) ! Some(0.00) ! None ! Some(
        12.99
      ) ! Some(coTstamp) ! (None, None, None, None).valid |
      "Convert to the same currency" !! Some("EUR") ! validAppKey ! Some(11.00) ! Some(2.67) ! Some(
        0.00
      ) ! Some("EUR") ! Some(12.99) ! Some(coTstamp) ! (
        Some("11.00"),
        Some("2.67"),
        Some("0.00"),
        Some("12.99")
      ).valid |
      "Valid APP ID and API key" !! Some("GBP") ! validAppKey ! Some(16.00) ! Some(2.67) ! Some(
        0.00
      ) ! None ! Some(10.00) ! Some(coTstamp) ! (Some("18.54"), Some("3.09"), Some("0.00"), None).valid |> {
      (
        _,
        trCurrency,
        apiKey,
        trAmountTotal,
        trAmountTax,
        trAmountShipping,
        tiCurrency,
        tiPrice,
        dateTime,
        expected
      ) =>
        (for {
          c <- Eval.now(
            CurrencyConversionConf(schemaKey, DeveloperAccount, apiKey, CurrencyUnit.EUR)
          )
          e <- c.enrichment[Eval]
          res <- e.convertCurrencies(
            trCurrency,
            trAmountTotal,
            trAmountTax,
            trAmountShipping,
            tiCurrency,
            tiPrice,
            dateTime
          )
        } yield res).value must_== expected
    }
}
