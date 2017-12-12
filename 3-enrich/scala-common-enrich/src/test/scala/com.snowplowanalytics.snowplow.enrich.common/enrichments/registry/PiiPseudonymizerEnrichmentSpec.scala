/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
import com.snowplowanalytics.iglu.client.SchemaCriterion
import org.specs2.Specification
import org.specs2.scalaz.ValidationMatchers

// Scala
import org.json4s.jackson.JsonMethods.parse

// Java
import java.net.URI
import org.joda.time.DateTime
import java.security.MessageDigest

// Snowplow
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.repositories.{EmbeddedRepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.snowplow.enrich.common.loaders.{CollectorApi,    CollectorContext, CollectorPayload, CollectorSource}
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.SpecHelpers.toNameValuePairs
import com.snowplowanalytics.snowplow.enrich.common.utils.TestResourcesRepositoryRef

// Scalaz
import scalaz.Scalaz._

class PiiPseudonymizerEnrichmentSpec extends Specification with ValidationMatchers {
  def is = s2"""
    This is a specification to test PII
    Hashing configured fields in POJO should work                                                           $e1
    Hashing configured fields in JSON should work in the simplest case and not affect anything else         $e2
    Hashing configured fields in JSON should work when the field is not there in a message                  $e3
    Hashing configured fields in JSON should work when multiple fields are matched through jsonpath         $e4
    Hashing configured fields in JSON should work when multiple fields are matched through schemacriterion  $e5
    Hashing configured fields in JSON should silently ignore unsupported types                              $e6
  """

  def commonSetup(enrichmentMap: EnrichmentMap): List[ValidatedEnrichedEvent] = {
    val registry = EnrichmentRegistry(enrichmentMap)
    val context  = CollectorContext(Some(DateTime.parse("2017-07-14T03:39:39.000+00:00")), Some("127.0.0.1"), None, None, Nil, None)
    val source   = CollectorSource("clj-tomcat", "UTF-8", None)
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
        |        "someInt": 1
        |      },
        |      "schema":  "iglu:com.acme/email_sent/jsonschema/1-1-0"
        |    }
        |  ]
        |}
      """.stripMargin
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

  private val ipEnrichment = IpLookupsEnrichment(Some(("geo", new URI("/ignored-in-local-mode/"), "GeoIPCity.dat")),
                                                 Some(("isp", new URI("/ignored-in-local-mode/"), "GeoIPISP.dat")),
                                                 None,
                                                 None,
                                                 None,
                                                 true)

  def e1 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiPojo(strategy = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")), fieldName = "user_id"),
          PiiPojo(strategy = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")), fieldName = "user_ipaddress"),
          PiiPojo(strategy = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")), fieldName = "ip_domain"),
          PiiPojo(strategy = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")), fieldName = "user_fingerprint")
        )))
    )
    val output   = commonSetup(enrichmentMap = enrichmentMap)
    val expected = new EnrichedEvent()
    expected.app_id           = "ads"
    expected.user_id          = "4b2d8785b49bad23638b17d8db76857a79bf79441241a78a97d88cc64bbf766e"
    expected.user_ipaddress   = "36595ea260a82b7e2d7cf44121892bf31031a9c27077d8c802454464178456c2"
    expected.ip_domain        = null
    expected.user_fingerprint = "9f9fc89b7a5428f2646347974404650fc8776f791afc2200efc8a82aa754e7e6"
    expected.geo_city         = "Delray Beach"
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
            strategy        = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")),
            fieldName       = "contexts",
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-0-*").toOption.get,
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
          (((contextJ \ "data")(0) \ "data" \ "emailAddress2").extract[String] must_== "bob@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress").extract[String] must_== "tim@acme.com") and
          (((contextJ \ "data")(1) \ "data" \ "emailAddress2").extract[String] must_== "tom@acme.com")
      }
    }
  }

  def e3 = {
    val enrichmentMap = Map(
      ("ip_lookups" -> ipEnrichment),
      ("pii_enrichment_config" -> PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            strategy        = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")),
            fieldName       = "contexts",
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
            strategy        = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")),
            fieldName       = "contexts",
            schemaCriterion = SchemaCriterion.parse("iglu:com.acme/email_sent/jsonschema/1-0-*").toOption.get,
            jsonPath        = "$.['emailAddress', 'emailAddress2']"
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
            strategy        = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")),
            fieldName       = "contexts",
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
            strategy        = PiiStrategyPseudonymize(hashFunction = MessageDigest.getInstance("SHA-256")),
            fieldName       = "contexts",
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
