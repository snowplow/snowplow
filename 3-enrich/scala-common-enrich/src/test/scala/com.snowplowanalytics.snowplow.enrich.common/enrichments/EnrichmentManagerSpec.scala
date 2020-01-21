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

import org.specs2.mutable.Specification
import org.specs2.matcher.EitherMatchers

import cats.Eval
import cats.implicits._
import cats.data.NonEmptyList

import io.circe.literal._

import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.badrows._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

import loaders._
import adapters.RawEvent
import utils.Clock._
import utils.ConversionUtils
import enrichments.registry.JavascriptScriptEnrichment

class EnrichmentManagerSpec extends Specification with EitherMatchers {
  val enrichmentReg = EnrichmentRegistry[Eval]()
  val client = SpecHelpers.client
  val processor = Processor("ssc-tests", "0.0.0")
  val timestamp = DateTime.now()

  val api = CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2")
  val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
  val context = CollectorPayload.Context(
    DateTime.parse("2013-08-29T00:18:48.000+00:00").some,
    "37.157.33.123".some,
    None,
    None,
    Nil,
    None
  )

  "enrichEvent" should {
    "return a SchemaViolations bad row if the input event contains an invalid context" >> {
      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" -> """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "foo": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )

      enriched.value.value must beLeft.like {
        case _: BadRow.SchemaViolations => ok
        case br => ko(s"bad row [$br] is not SchemaViolations")
      }
    }

    "return a SchemaViolations bad row if the input unstructured event is invalid" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "ue_pr" -> """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org",
                "emailAddress3": "foo@bar.org"
              }
            }
          }"""
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value.value must beLeft.like {
        case _: BadRow.SchemaViolations => ok
        case br => ko(s"bad row [$br] is not SchemaViolations")
      }
    }

    "return an EnrichmentFailures bad row if one of the enrichment (JS enrichment here) fails" >> {
      val script = """
        function process(event) {
          throw "Javascript exception";
          return [ { a: "b" } ];
        }"""

      val config = json"""{
        "parameters": {
          "script": ${ConversionUtils.encodeBase64Url(script)}
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val jsEnrichConf =
        JavascriptScriptEnrichment.parse(config, schemaKey).toOption.get
      val jsEnrich = JavascriptScriptEnrichment(jsEnrichConf.schemaKey, jsEnrichConf.script)
      val enrichmentReg = EnrichmentRegistry[Eval](javascriptScript = Some(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value.value must beLeft.like {
        case BadRow.EnrichmentFailures(
            _,
            Failure.EnrichmentFailures(
              _,
              NonEmptyList(
                FailureDetails.EnrichmentFailure(
                  _,
                  _: FailureDetails.EnrichmentFailureMessage.Simple
                ),
                Nil
              )
            ),
            _
            ) =>
          ok
        case br =>
          ko(
            s"bad row [$br] is not an EnrichmentFailures containing one EnrichmentFailureMessage.Simple"
          )
      }
    }

    "return an EnrichmentFailures bad row containing one IgluError if one of the contexts added by the enrichments is invalid" >> {
      val script = """
        function process(event) {
          return [ { schema: "iglu:com.acme/email_sent/jsonschema/1-0-0",
                     data: {
                       emailAddress: "hello@world.com",
                       foo: "bar"
                     }
                   } ];
        }"""

      val config = json"""{
        "parameters": {
          "script": ${ConversionUtils.encodeBase64Url(script)}
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      )
      val jsEnrichConf =
        JavascriptScriptEnrichment.parse(config, schemaKey).toOption.get
      val jsEnrich = JavascriptScriptEnrichment(jsEnrichConf.schemaKey, jsEnrichConf.script)
      val enrichmentReg = EnrichmentRegistry[Eval](javascriptScript = Some(jsEnrich))

      val parameters = Map(
        "e" -> "pp",
        "tv" -> "js-0.13.1",
        "p" -> "web"
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value.value must beLeft.like {
        case BadRow.EnrichmentFailures(
            _,
            Failure.EnrichmentFailures(
              _,
              NonEmptyList(
                FailureDetails.EnrichmentFailure(
                  _,
                  _: FailureDetails.EnrichmentFailureMessage.IgluError
                ),
                Nil
              )
            ),
            _
            ) =>
          ok
        case br => ko(s"bad row [$br] is not an EnrichmentFailures containing one IgluError")
      }
    }

    "emit an EnrichedEvent if everything goes well" >> {
      val parameters = Map(
        "e" -> "ue",
        "tv" -> "js-0.13.1",
        "p" -> "web",
        "co" -> """
          {
            "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
            "data": [
              {
                "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
                "data": {
                  "emailAddress": "hello@world.com",
                  "emailAddress2": "foo@bar.org"
                }
              }
            ]
          }
        """,
        "ue_pr" -> """
          {
            "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
            "data":{
              "schema":"iglu:com.acme/email_sent/jsonschema/1-0-0",
              "data": {
                "emailAddress": "hello@world.com",
                "emailAddress2": "foo@bar.org"
              }
            }
          }"""
      )
      val rawEvent = RawEvent(api, parameters, None, source, context)
      val enriched = EnrichmentManager.enrichEvent(
        enrichmentReg,
        client,
        processor,
        timestamp,
        rawEvent
      )
      enriched.value.value must beRight
    }
  }
}
