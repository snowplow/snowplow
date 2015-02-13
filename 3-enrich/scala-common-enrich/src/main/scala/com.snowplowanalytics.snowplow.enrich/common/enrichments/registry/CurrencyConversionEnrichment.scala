/*
 * Copyright (c) 2012-2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics
package snowplow
package enrich
package common
package enrichments
package registry

// This project
import utils.MapTransformer._

// Java
import java.lang.{Integer => JInteger}
import java.math.{BigDecimal => JBigDecimal}
import java.lang.{Byte => JByte}
import java.net.URI
import java.net._

// Maven Artifact
import org.apache.maven.artifact.versioning.DefaultArtifactVersion

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.JValue

// Iglu
import iglu.client.SchemaKey
import iglu.client.validation.ProcessingMessageMethods._

// Joda-Money
import org.joda.money.{Money}
import org.joda.time.{DateTime, DateTimeZone}

// Scala-Forex
import com.snowplowanalytics.forex._
import com.snowplowanalytics.forex.oerclient.{OerClientConfig, DeveloperAccount, AccountType, OerResponseError}
import com.snowplowanalytics.forex.oerclient.OerResponseError._
import com.snowplowanalytics.forex.{Forex, ForexConfig}
import com.snowplowanalytics.forex.ForexLookupWhen._
import com.snowplowanalytics.forex.oerclient._

// This project
import common.utils.ConversionUtils
import utils.ScalazJson4sUtils

/**
 * Companion object. Lets us create an CurrencyConversionEnrichment
 * instance from a JValue.
 */
object CurrencyConversionEnrichmentConfig extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "currency_conversion_config", "jsonschema", "1-0-0")
  
  //Creates an CurrencyConversionEnrichment instance from a JValue
  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[CurrencyConversionEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        apiKey        <- ScalazJson4sUtils.extract[String](config, "parameters", "apiKey")
        baseCurrency  <- ScalazJson4sUtils.extract[String](config, "parameters", "baseCurrency")
        rateAt        <- ScalazJson4sUtils.extract[String](config, "parameters", "rateAt")
        enrich        =  CurrencyConversionEnrichment(apiKey, baseCurrency, rateAt)
      } yield enrich).toValidationNel
    })
  }
}

// Object and a case object with the same name
case class CurrencyConversionEnrichment(
  apiKey: String,
  baseCurrency: String, 
  rateAt: String) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  // To provide Validation for org.joda.money.Money variable
  def eitherToValidation(input:Either[OerResponseError, Money]): Validation[String, Option[String]]= {
    input match {
      case Right(l) => (l.getAmount().toPlainString()).some.success
      case Left(l) => (s"Open Exchange Rates error, message: ${l.errorMessage}").failure
    }
  }
  
  def getEitherValidation(fx: Forex, trCurrency: Option[String], trTotal: Option[Double], tstamp: DateTime): Validation[String, Option[String]] = {
      trCurrency match {
        case Some(trCurr) =>{ 
          trTotal match{
            case Some(trC) => {
              eitherToValidation(fx.convert(trC, trCurr).to(baseCurrency).at(tstamp))
            } 
            case None => None.success                                            
          }
        }
        case None => None.success
      } 
  }
  /**
  * Convert's currency for a given
  * set of currency, using
  * Scala-Forex.
  *
  * @param trCurrency The desired
  *        currency for a given 
  *        amount
  * @param trAmounts Contains
  *        total amount, tax, shipping
  *        amount's
  * @param tiCurrency Trasaction Item
  *        Currency
   * @param tiPrice Trasaction Item
  *         Price
  * @return the converted currency
  *         in TransacrionAmounts
  *         format and Ttansaction
  *         Item Price 
  */
  def convertCurrencies(trCurrency: Option[String], trTotal: Option[Double], trAmountsTax: Option[Double], trAmountsShipping: Option[Double], tiCurrency: Option[String], tiPrice: Option[Double], collectorTstamp: Option[DateTime]): ValidationNel[String, (Option[String], Option[String], Option[String], Option[String])] = {
    val check = Double.NaN
    try{
        val fx = Forex(ForexConfig( nowishCacheSize = 0, nowishSecs = 0, eodCacheSize= 0), OerClientConfig(apiKey, DeveloperAccount))  
                collectorTstamp match {
                  case Some(tstamp) =>{
                    val newCurrencyTr  = getEitherValidation(fx, trCurrency, trTotal, tstamp)  
                    val newCurrencyTi = getEitherValidation(fx, tiCurrency, tiPrice, tstamp)
                    val newTrAmountsTax = getEitherValidation(fx, trCurrency, trAmountsTax, tstamp) 
                    val newTrAmountsShipping = getEitherValidation(fx, trCurrency, trAmountsShipping, tstamp) 
                    (newCurrencyTr.toValidationNel  |@| newTrAmountsTax.toValidationNel  |@| newTrAmountsShipping.toValidationNel |@| newCurrencyTi.toValidationNel) {
                      (_, _, _, _)
                    }       
                    
                  }
                  case None => "DateTime Missing".failNel 
                }
    } catch {
      case e : NoSuchElementException =>"Provided Currency not supported : %s".format(e).failNel
      case f : UnknownHostException => "Could not extract Convert Currencies from OER Service :%s".format(f).failNel
      case g => "Exception Converting Currency :%s".format(g).failNel
    }

  }
}

