/*
 * Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.enrich
package common
package enrichments
package registry

// Specs2 & Scalaz-Specs2
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// Scala
import org.json4s.jackson.JsonMethods.parse

// Java
import java.net.URI
import org.joda.time.DateTime
import org.apache.commons.codec.digest.DigestUtils

// Snowplow
import common.loaders.{CollectorApi, CollectorContext, CollectorPayload, CollectorSource}
import common.outputs.EnrichedEvent
import common.SpecHelpers.toNameValuePairs
import common.utils.TestResourcesRepositoryRef

// Iglu
import com.snowplowanalytics.iglu.client.SchemaCriterion
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.repositories.{EmbeddedRepositoryRef, RepositoryRefConfig}

// Scalaz
import scalaz.Scalaz._

import PiiConstants._

class PiiPseudonymizerEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
    This is a specification to test PII
    Hashing configured scalar fields in POJO should work                                                         $e1
    Hashing configured JSON fields in POJO should work in the simplest case and not affect anything else         $e2
    Hashing configured JSON fields in POJO should work when the field is not there in a message                  $e3
    Hashing configured JSON fields in POJO should work when multiple fields are matched through jsonpath         $e4
    Hashing configured JSON fields in POJO should work when multiple fields are matched through schemacriterion  $e5
    Hashing configured JSON fields in POJO should silently ignore unsupported types                              $e6
  """

  def commonSetup(enrichmentMap: EnrichmentMap): List[ValidatedEnrichedEvent] = {
    val registry = EnrichmentRegistry(enrichmentMap)
    val context =
      CollectorContext(Some(DateTime.parse("2017-07-14T03:39:39.000+00:00")), Some("127.0.0.1"), None, None, Nil, None)
    val source = CollectorSource("clj-tomcat", "UTF-8", None)
    val collectorPayload = CollectorPayload(
      CollectorApi("com.snowplowanalytics.snowplow", "tp2"),
      toNameValuePairs(
        "e"   -> "se",
        "aid" -> "ads",
        "uid" -> "john@acme.com",
        "ip"  -> "70.46.123.145",
        "fp"  -> "its_you_again!",
        "co" ->
          """
        |{
        |  "schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
        |  "data":[
        |      {
        |      "schema":  "iglu:com.acme/email_sent/jsonschema/1-0-0",
        |      "data": {
        |        "emailAddress" : "jim@acme.com",
        |        "emailAddress2" : "bob@acme.com"
        |      }
        |    },
        |    {
        |      "data": {
        |        "emailAddress" : "tim@acme.com",
        |        "emailAddress2" : "tom@acme.com",
        |        "schema": "iglu:com.acme/email_sent/jsonschema/1-0-0",
        |        "data": {
        |          "emailAddress" : "jim@acme.com",
        |          "emailAddress2" : "bob@acme.com"
        |        },
        |        "someInt": 1
        |      },
        |      "schema":  "iglu:com.acme/email_sent/jsonschema/1-1-0"
        |    }
        |  ]
        |}
      """.stripMargin,
        "ue_pr" -> """
        |{
        |   "schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
        |   "data":{
        |     "schema":"iglu:com.mailgun/message_clicked/jsonschema/1-0-0",
        |     "data":{
        |       "recipient":"alice@example.com",
        |       "city":"San Francisco",
        |       "ip":"50.56.129.169",
        |       "myVar2":"awesome",
        |       "timestamp":"2016-06-30T14:31:09.000Z",
        |       "url":"http://mailgun.net",
        |       "userAgent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.43 Safari/537.31",
        |       "domain":"sandboxbcd3ccb1a529415db665622619a61616.mailgun.org",
        |       "signature":"ffe2d315a1d937bd09d9f5c35ddac1eb448818e2203f5a41e3a7bd1fb47da385",
        |       "country":"US",
        |       "clientType":"browser",
        |       "clientOs":"Linux",
        |       "token":"cd89cd860be0e318371f4220b7e0f368b60ac9ab066354737f",
        |       "clientName":"Chrome",
        |       "region":"CA",
        |       "deviceType":"desktop",
        |       "myVar1":"Mailgun Variable #1"
        |     }
        |   }
        |}""".stripMargin.replaceAll("[\n\r]", "")
      ),
      None,
      None,
      source,
      context
    )
    val input: ValidatedMaybeCollectorPayload = Some(collectorPayload).successNel
    val rrc                                   = new RepositoryRefConfig("test-schema", 1, List("com.snowplowanalytics.snowplow"))
    val repos                                 = TestResourcesRepositoryRef(rrc, "src/test/resources/iglu-schemas")
    implicit val resolver                     = new Resolver(repos = List(repos))
    EtlPipeline.processEvents(registry, s"spark-0.0.0", new DateTime(1500000000L), input)
  }

  private val ipEnrichment = IpLookupsEnrichment(Some(("geo", new URI("/ignored-in-local-mode/"), "GeoIP2-City.mmdb")),
                                                 Some(("isp", new URI("/ignored-in-local-mode/"), "GeoIP2-ISP.mmdb")),
                                                 None,
                                                 None,
                                                 true)

  def e1 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiScalar(strategy     = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
                    fieldMutator = ScalarMutators.get("user_id").get),
          PiiScalar(strategy     = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
                    fieldMutator = ScalarMutators.get("user_ipaddress").get),
          PiiScalar(strategy     = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
                    fieldMutator = ScalarMutators.get("ip_domain").get),
          PiiScalar(strategy     = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
                    fieldMutator = ScalarMutators.get("user_fingerprint").get)
        )))
    )
    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "4b2d8785b49bad23638b17d8db76857a79bf79441241a78a97d88cc64bbf766e"
    expected.user_ipaddress   = "36595ea260a82b7e2d7cf44121892bf31031a9c27077d8c802454464178456c2"
    expected.ip_domain        = null
    expected.user_fingerprint = "9f9fc89b7a5428f2646347974404650fc8776f791afc2200efc8a82aa754e7e6"
    expected.geo_city         = null
    expected.etl_tstamp       = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    output.size must_== 1
    val out = output(0)
    out must beSuccessful.like {
      case enrichedEvent => {
        (enrichedEvent.app_id must_== expected.app_id) and
          (enrichedEvent.user_id must_== expected.user_id) and
          (enrichedEvent.user_ipaddress must_== expected.user_ipaddress) and
          (enrichedEvent.ip_domain must_== expected.ip_domain) and
          (enrichedEvent.user_fingerprint must_== expected.user_fingerprint) and
          (enrichedEvent.geo_city must_== expected.geo_city) and
          (enrichedEvent.etl_tstamp must_== expected.etl_tstamp) and
          (enrichedEvent.collector_tstamp must_== expected.collector_tstamp)
      }
    }
  }

  def e2 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-0-*").toOption.get,
            jsonPath        = "$.emailAddress"
          ),
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-1-0").toOption.get,
            jsonPath        = "$.data.emailAddress2"
          ),
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("unstruct_event").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.mailgun/message_clicked/jsonschema/1-0-0").toOption.get,
            jsonPath        = "$.ip"
          )
        )))
    )

    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "john@acme.com"
    expected.user_ipaddress   = "70.46.123.145"
    expected.ip_domain        = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city         = "Delray Beach"
    expected.etl_tstamp       = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    output.size must_== 1
    val out = output(0)
    out must beSuccessful.like {
      case enrichedEvent => {
        implicit val formats = org.json4s.DefaultFormats
        val contextJ         = parse(enrichedEvent.contexts)
        val unstructEventJ   = parse(enrichedEvent.unstruct_event)
        (((contextJ \ "data")(0) \ "data" \ "emailAddress")
          .extract[String] must_== "3571b422ecb9ac85cb654b2fce521ae351d4695b0fb788aac75caf724e7881f0") and
          (((contextJ \ "data")(0) \ "data" \ "emailAddress2").extract[String] must_== "bob@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress").extract[String] must_== "tim@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress2").extract[String] must_== "tom@acme.com") and
          // The following three tests are for the case that the context schema allows the fields data and schema
          // and in addition the schema field matches the configured schema. There should be no replacement there
          // (unless that is specified in jsonpath)
          (((contextJ \ "data")(1) \ "data" \ "data" \ "emailAddress").extract[String] must_== "jim@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "data" \ "emailAddress2")
            .extract[String] must_== "405ac8384fa984f787f9486daf34d84d98f20c4d6a12e2cc4ed89be3bcb06ad6") and
          (((contextJ \ "data")(1) \ "data" \ "schema")
            .extract[String] must_== "iglu:com.acme/email_sent/jsonschema/1-0-0") and
          (((unstructEventJ \ "data") \ "data" \ "ip")
            .extract[String] must_== "b5814ada7bb3abb2ed7f8713433a60ed3b3780f7d98a95c936cc62abb16f316f") and
          (((unstructEventJ \ "data") \ "data" \ "myVar2").extract[String] must_== "awesome")
      }
    }
  }

  def e3 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-*-*").toOption.get,
            jsonPath        = "$.field.that.does.not.exist.in.this.instance"
          )
        )))
    )

    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "john@acme.com"
    expected.user_ipaddress   = "70.46.123.145"
    expected.ip_domain        = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city         = "Delray Beach"
    expected.etl_tstamp       = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    output.size must_== 1
    val out = output(0)
    out must beSuccessful.like {
      case enrichedEvent => {
        implicit val formats = org.json4s.DefaultFormats
        val contextJ         = parse(enrichedEvent.contexts)
        (((contextJ \ "data")(0) \ "data" \ "emailAddress").extract[String] must_== "jim@acme.com") and
          (((contextJ \ "data")(0) \ "data" \ "emailAddress2").extract[String] must_== "bob@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress").extract[String] must_== "tim@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress2").extract[String] must_== "tom@acme.com")
      }
    }
  }

  def e4 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-0-*").toOption.get,
            jsonPath        = "$.['emailAddress', 'emailAddress2', 'emailAddressNonExistent']" // Last case throws an exeption if misconfigured
          )
        )))
    )

    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "john@acme.com"
    expected.user_ipaddress   = "70.46.123.145"
    expected.ip_domain        = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city         = "Delray Beach"
    expected.etl_tstamp       = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    output.size must_== 1
    val out = output(0)
    out must beSuccessful.like {
      case enrichedEvent => {
        implicit val formats = org.json4s.DefaultFormats
        val contextJ         = parse(enrichedEvent.contexts)
        (((contextJ \ "data")(0) \ "data" \ "emailAddress")
          .extract[String] must_== "3571b422ecb9ac85cb654b2fce521ae351d4695b0fb788aac75caf724e7881f0") and
          (((contextJ \ "data")(0) \ "data" \ "emailAddress2")
            .extract[String] must_== "405ac8384fa984f787f9486daf34d84d98f20c4d6a12e2cc4ed89be3bcb06ad6") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress").extract[String] must_== "tim@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress2").extract[String] must_== "tom@acme.com")
      }
    }
  }

  def e5 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-*-0").toOption.get,
            jsonPath        = "$.emailAddress"
          )
        )))
    )
    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "john@acme.com"
    expected.user_ipaddress   = "70.46.123.145"
    expected.ip_domain        = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city         = "Delray Beach"
    expected.etl_tstamp       = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    output.size must_== 1
    val out = output(0)
    out must beSuccessful.like {
      case enrichedEvent => {
        implicit val formats = org.json4s.DefaultFormats
        val contextJ         = parse(enrichedEvent.contexts)
        (((contextJ \ "data")(0) \ "data" \ "emailAddress")
          .extract[String] must_== "3571b422ecb9ac85cb654b2fce521ae351d4695b0fb788aac75caf724e7881f0") and
          (((contextJ \ "data")(0) \ "data" \ "emailAddress2")
            .extract[String] must_== "bob@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress")
            .extract[String] must_== "663ea32adb6f26f7a025e3b6c850294d0d7755c3010c5e7a5fd690cfa5d2938f") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress2").extract[String] must_== "tom@acme.com")
      }
    }
  }

  def e6 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = (b: Array[Byte]) => DigestUtils.sha256Hex(b)),
            fieldMutator    = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-*-*").toOption.get,
            jsonPath        = "$.someInt"
          )
        )))
    )
    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "john@acme.com"
    expected.user_ipaddress   = "70.46.123.145"
    expected.ip_domain        = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city         = "Delray Beach"
    expected.etl_tstamp       = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    output.size must_== 1
    val out = output(0)
    out must beSuccessful.like {
      case enrichedEvent => {
        implicit val formats = org.json4s.DefaultFormats
        val contextJ         = parse(enrichedEvent.contexts)
        (((contextJ \ "data")(0) \ "data" \ "emailAddress")
          .extract[String] must_== "jim@acme.com") and
          (((contextJ \ "data")(0) \ "data" \ "emailAddress2")
            .extract[String] must_== "bob@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress")
            .extract[String] must_== "tim@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress2").extract[String] must_== "tom@acme.com")
        (((contextJ \ "data")(1) \ "data" \ "someInt").extract[Int] must_== 1)
      }
    }
  }
}
