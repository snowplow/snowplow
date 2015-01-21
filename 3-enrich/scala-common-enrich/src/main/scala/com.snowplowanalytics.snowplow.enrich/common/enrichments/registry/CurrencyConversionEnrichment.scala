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

// Java
import java.net.URI

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

// Scala Forex
import org.joda.time.{DateTime, DateTimeZone}
import com.snowplowanalytics.forex._
import com.snowplowanalytics.forex.oerclient.{OerClientConfig, DeveloperAccount, AccountType}
import com.snowplowanalytics.forex.{Forex, ForexConfig}
// This project
import common.utils.ConversionUtils
import utils.ScalazJson4sUtils

/**
* Companion object. Lets us create an CurrencyConversionEnrichment
* instance from a JValue.
*/
object CurrencyConversionEnrichmentConfig extends ParseableEnrichment {

  val supportedSchemaKey = SchemaKey("com.snowplowanalytics.snowplow", "currency_conversion_config", "jsonschema", "1-0-0")

  def parse(config: JValue, schemaKey: SchemaKey): ValidatedNelMessage[CurrencyConversionEnrichment] = {
    isParseable(config, schemaKey).flatMap( conf => {
      (for {
        apiKey    <- ScalazJson4sUtils.extract[String](config, "parameters", "apiKey")
        baseCurrency    <- ScalazJson4sUtils.extract[String](config, "parameters", "baseCurrency")
        rateAt      <- ScalazJson4sUtils.extract[String](config, "parameters", "rateAt")
        enrich =  CurrencyConversionEnrichment(apiKey, baseCurrency, rateAt)
      } yield enrich).toValidationNel
    })
  }
}

case class TransactionAmounts(
  total: String, 
  tax: String, 
  shipping: String)

case class OerClientConfig(
  appId: String,            
  accountLevel: AccountType 
) extends ForexClientConfig

// Object and a case object with the same name

case class CurrencyConversionEnrichment(
  apiKey: String, 
  baseCurrency: String, 
  rateAt: String) extends Enrichment {

  val version = new DefaultArtifactVersion("0.1.0")

  def convertCurrencies(trCurrency: String, trAmounts: TransactionAmounts, tiCurrency: String, tiPrice: String): Validation[NonEmptyList[String], (TransactionAmounts, String)] = {

    val fx = Forex(ForexConfig(), OerClientConfig(apiKey, DeveloperAccount))
    //val rate_tr = fx.rate(baseCurrency).to(trCurrency).now
    //val rate_ti = fx.rate(baseCurrency).to(tiCurrency).now
    val newCurrency_tr = fx.convert((trAmounts.total).toDouble, baseCurrency).to(trCurrency)
    val newCurrency_ti = fx.convert((tiPrice).toDouble, baseCurrency).to(tiCurrency)
    val returnTuple = (TransactionAmounts(newCurrency_tr.toString, trAmounts.tax, trAmounts.shipping), newCurrency_ti.toString)
    returnTuple.success
  }
}

