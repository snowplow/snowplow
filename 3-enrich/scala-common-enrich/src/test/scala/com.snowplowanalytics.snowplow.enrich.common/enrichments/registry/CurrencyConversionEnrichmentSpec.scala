/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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

// Specs2
import org.specs2.Specification
import org.specs2.matcher.DataTables
import org.specs2.scalaz._

// Scalaz
import scalaz._
import Scalaz._

// Joda Time
import org.joda.time.DateTime

/**
 * Tests the convertCurrencies function
 */
class CurrencyConversionEnrichmentSpec extends Specification with DataTables { def is =

  if (sys.env.get("OER_KEY").isDefined) {
    "This is a specification to test convertCurrencies"                       ^
                                                                             p^
          "Failure test for Currency Conversion"                              ! e1^
          "Success test for Currency Conversion"                              ! e2^
                                                                              end
  } else {
    "WARNING: Skipping CurrencyConversionEnrichment test as no OER_KEY environment variable was found for authentication" ^
                                                                                                                         p^
                                                                                                                          end
  }

  val appId = sys.env.get("OER_KEY").getOrElse("---")
  val trCurrencyMissing = Failure(NonEmptyList("Open Exchange Rates error, message: Currency [] is not supported by Joda money Currency not found in the API, invalid currency ", "Open Exchange Rates error, message: Currency [] is not supported by Joda money Currency not found in the API, invalid currency ", "Open Exchange Rates error, message: Currency [] is not supported by Joda money Currency not found in the API, invalid currency "))
  val currencyInvalidRup = Failure(NonEmptyList("Open Exchange Rates error, message: Currency [RUP] is not supported by Joda money Currency not found in the API, invalid currency ", "Open Exchange Rates error, message: Currency [RUP] is not supported by Joda money Currency not found in the API, invalid currency ", "Open Exchange Rates error, message: Currency [RUP] is not supported by Joda money Currency not found in the API, invalid currency "))
  val currencyInvalidHul = Failure(NonEmptyList("Open Exchange Rates error, message: Currency [HUL] is not supported by Joda money Currency not found in the API, invalid currency "))
  val appIdInvalid = Failure(NonEmptyList("Open Exchange Rates error, message: Invalid App ID provided - please sign up at https://openexchangerates.org/signup, or contact support@openexchangerates.org. Thanks!"))
  val coTstamp: DateTime = new DateTime(2011, 3, 13, 0, 0)

  def e1 =
    "SPEC NAME"                         || "TRANSACTION CURRENCY"         | "API KEY"                                | "TOTAL AMOUNT"                     |"TOTAL TAX"                      |"SHIPPING"                           | "TRANSACTION ITEM CURRENCY"           | "TRANSACTION ITEM PRICE"              | "DATETIME"              |"CONVERTED TUPLE"                               |
    "Invalid transaction currency"      !! Some("RUP")                    ! appId                                    ! Some(11.00)                        ! Some(1.17)                      ! Some(0.00)                          ! None                                  ! Some(17.99)                           ! Some(coTstamp)          ! currencyInvalidRup                             |
    "Invalid transaction item currency" !! None                           ! appId                                    ! Some(12.00)                        ! Some(0.7)                       ! Some(0.00)                          ! Some("HUL")                           ! Some(1.99)                            ! Some(coTstamp)          ! currencyInvalidHul                             |
    "Invalid OER API key"               !! None                           ! "8A8A8A8A8A8A8A8A8A8A8A8AA8A8A8A8"       ! Some(13.00)                        ! Some(3.67)                      ! Some(0.00)                          ! Some("GBP")                           ! Some(2.99)                            ! Some(coTstamp)          ! appIdInvalid                                   |> {
      (_, trCurrency, apiKey, trAmountTotal, trAmountTax, trAmountShipping, tiCurrency, tiPrice, dateTime, expected) =>
        CurrencyConversionEnrichment(apiKey, "EUR", "EOD_PRIOR").convertCurrencies(trCurrency, trAmountTotal, trAmountTax, trAmountShipping, tiCurrency, tiPrice, dateTime) must_== expected
    }

  def e2 =
    "SPEC NAME"                                  || "TRANSACTION CURRENCY"   | "API KEY"                             | "TOTAL AMOUNT"   |"TOTAL TAX"  |"SHIPPING"   | "TRANSACTION ITEM CURRENCY"     | "TRANSACTION ITEM PRICE"       | "DATETIME"         |"CONVERTED TUPLE"                                           |
    "All fields absent"                          !! None                     ! appId                                 ! None             ! None        ! None        ! None                            ! None                           ! None               ! Failure(NonEmptyList("Collector timestamp missing"))       |
    "All fields absent except currency"          !! Some("GBP")              ! appId                                 ! None             ! None        ! None        ! Some("GBP")                     ! None                           ! None               ! Failure(NonEmptyList("Collector timestamp missing"))       |
    "Transaction Currency Tax and Shipping Null" !! Some("GBP")              ! appId                                 ! Some(11.00)      ! None        ! None        ! None                            ! None                           ! Some(coTstamp)     ! (Some("12.75"),None,None,None).success                     |
    "Transaction Currency Total Null"            !! Some("GBP")              ! appId                                 ! None             ! Some(2.67)  ! Some(0.00)  ! None                            ! None                           ! Some(coTstamp)     ! (None, Some("3.09"), Some("0.00"), None).success           |
    "Transaction Currency Null"                  !! None                     ! appId                                 ! None             ! None        ! None        ! Some("GBP")                     ! Some(12.99)                    ! Some(coTstamp)     ! (None,None,None,Some("15.05")).success                     |
    "Transaction Item Null"                      !! Some("GBP")              ! appId                                 ! Some(11.00)      ! Some(2.67)  ! Some(0.00)  ! None                            ! None                           ! Some(coTstamp)     ! (Some("12.75"), Some("3.09"), Some("0.00"),None).success   |
    "Valid APP ID and API key"                   !! None                     ! appId                                 ! Some(14.00)      ! Some(4.67)  ! Some(0.00)  ! Some("GBP")                     ! Some(10.99)                    ! Some(coTstamp)     ! ( None, None, None, Some("12.74")).success                 |
    "Both Currency Null"                         !! None                     ! appId                                 ! Some(11.00)      ! Some(2.67)  ! Some(0.00)  ! None                            ! Some(12.99)                    ! Some(coTstamp)     ! (None,None,None,None).success                              |
    "Valid APP ID and API key"                   !! Some("GBP")              ! appId                                 ! Some(16.00)      ! Some(2.67)  ! Some(0.00)  ! None                            ! Some(10.00)                    ! Some(coTstamp)     ! (Some("18.54"), Some("3.09"), Some("0.00"),None).success   |> {
      (_, trCurrency, apiKey, trAmountTotal, trAmountTax, trAmountShipping, tiCurrency, tiPrice, dateTime, expected) =>
        CurrencyConversionEnrichment(apiKey, "EUR", "EOD_PRIOR").convertCurrencies(trCurrency, trAmountTotal, trAmountTax, trAmountShipping, tiCurrency, tiPrice, dateTime) must_== expected
    }
}

