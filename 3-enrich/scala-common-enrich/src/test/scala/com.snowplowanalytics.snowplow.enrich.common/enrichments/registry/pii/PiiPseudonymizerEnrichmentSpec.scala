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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii

import cats.Id
import cats.data.Validated
import cats.syntax.option._
import cats.syntax.validated._

import io.circe.Json
import io.circe.literal._
import io.circe.parser._

import org.joda.time.DateTime

import org.apache.commons.codec.digest.DigestUtils

import com.snowplowanalytics.iglu.core._

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.Registry
import com.snowplowanalytics.iglu.client.validator.CirceValidator

import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor}

import com.snowplowanalytics.snowplow.enrich.common.{EtlPipeline, SpecHelpers}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.IpLookupsEnrichment
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.loaders._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.Clock._

import org.specs2.Specification
import org.specs2.matcher.ValidatedMatchers

class PiiPseudonymizerEnrichmentSpec extends Specification with ValidatedMatchers {
  def is = s2"""
  Hashing configured scalar fields in POJO should work                                                        $e1
  Hashing configured JSON fields in POJO should work in the simplest case and not affect anything else        $e2
  Hashing configured JSON fields in POJO should work when the field is not there in a message                 $e3
  Hashing configured JSON fields in POJO should work when multiple fields are matched through jsonpath        $e4
  Hashing configured JSON fields in POJO should work when multiple fields are matched through schemacriterion $e5
  Hashing configured JSON fields in POJO should silently ignore unsupported types                             $e6
  Hashing configured JSON and scalar fields in POJO emits a correct pii_transformation event                  $e7
  Hashing configured JSON fields in POJO should not create new fields                                         $e8
  """

  def commonSetup(enrichmentReg: EnrichmentRegistry[Id]): List[Validated[BadRow, EnrichedEvent]] = {
    val context =
      CollectorPayload.Context(
        Some(DateTime.parse("2017-07-14T03:39:39.000+00:00")),
        Some("127.0.0.1"),
        None,
        None,
        Nil,
        None
      )
    val source = CollectorPayload.Source("clj-tomcat", "UTF-8", None)
    val collectorPayload = CollectorPayload(
      CollectorPayload.Api("com.snowplowanalytics.snowplow", "tp2"),
      SpecHelpers.toNameValuePairs(
        "e" -> "se",
        "aid" -> "ads",
        "uid" -> "john@acme.com",
        "ip" -> "70.46.123.145",
        "fp" -> "its_you_again!",
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
    val input = Some(collectorPayload).validNel
    val regConf = Registry.Config(
      "test-schema",
      0,
      List("com.snowplowanalytics.snowplow", "com.acme", "com.mailgun")
    )
    val reg = Registry.Embedded(regConf, path = "/iglu-schemas")
    val client = Client[Id, Json](Resolver(List(reg), None), CirceValidator)
    EtlPipeline
      .processEvents[Id](
        new AdapterRegistry(),
        enrichmentReg,
        client,
        Processor("spark", "0.0.0"),
        new DateTime(1500000000L),
        input
      )
  }

  private val ipEnrichment = {
    val js = json"""{
      "enabled": true,
      "parameters": {
        "geo": {
          "database": "GeoIP2-City.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        },
        "isp": {
          "database": "GeoIP2-ISP.mmdb",
          "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        }
      }
    }"""
    val schemaKey = SchemaKey(
      "com.snowplowanalytics.snowplow",
      "ip_lookups",
      "jsonschema",
      SchemaVer.Full(2, 0, 0)
    )
    IpLookupsEnrichment.parse(js, schemaKey, true).toOption.get.enrichment[Id]
  }

  def e1 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiScalar(fieldMutator = ScalarMutators("user_id")),
          PiiScalar(
            fieldMutator = ScalarMutators("user_ipaddress")
          ),
          PiiScalar(fieldMutator = ScalarMutators("ip_domain")),
          PiiScalar(
            fieldMutator = ScalarMutators("user_fingerprint")
          )
        ),
        false,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )
    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "7d8a4beae5bc9d314600667d2f410918f9af265017a6ade99f60a9c8f3aac6e9"
    expected.user_ipaddress = "dd9720903c89ae891ed5c74bb7a9f2f90f6487927ac99afe73b096ad0287f3f5"
    expected.ip_domain = null
    expected.user_fingerprint = "27abac60dff12792c6088b8d00ce7f25c86b396b8c3740480cd18e21068ecff4"
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        (enrichedEvent.app_id must_== expected.app_id) and
          (enrichedEvent.user_id must_== expected.user_id) and
          (enrichedEvent.user_ipaddress must_== expected.user_ipaddress) and
          (enrichedEvent.ip_domain must_== expected.ip_domain) and
          (enrichedEvent.user_fingerprint must_== expected.user_fingerprint) and
          (enrichedEvent.geo_city must_== expected.geo_city) and
          (enrichedEvent.etl_tstamp must_== expected.etl_tstamp) and
          (enrichedEvent.collector_tstamp must_== expected.collector_tstamp)
    }
    size and validOut
  }

  def e2 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0),
            jsonPath = "$.emailAddress"
          ),
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 1, 0),
            jsonPath = "$.data.emailAddress2"
          ),
          PiiJson(
            fieldMutator = JsonMutators("unstruct_event"),
            schemaCriterion = SchemaCriterion("com.mailgun", "message_clicked", "jsonschema", 1, 0, 0),
            jsonPath = "$.ip"
          )
        ),
        false,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )

    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "john@acme.com"
    expected.user_ipaddress = "70.46.123.145"
    expected.ip_domain = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor
        val contextJFirstElement = contextJ.downField("data").downArray
        val contextJSecondElement = contextJFirstElement.right
        val unstructEventJ = parse(enrichedEvent.unstruct_event).toOption.get.hcursor
          .downField("data")
          .downField("data")

        val first = (contextJFirstElement
          .downField("data")
          .get[String]("emailAddress") must beRight(
          "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
        )) and
          (contextJFirstElement.downField("data").get[String]("emailAddress2") must beRight(
            "bob@acme.com"
          )) and
          (contextJSecondElement.downField("data").get[String]("emailAddress") must beRight(
            "tim@acme.com"
          )) and
          (contextJSecondElement.downField("data").get[String]("emailAddress2") must beRight(
            "tom@acme.com"
          ))

        // The following three tests are for the case that the context schema allows the fields
        // data and schema and in addition the schema field matches the configured schema. There
        // should be no replacement there (unless that is specified in jsonpath)
        val second = (contextJSecondElement
          .downField("data")
          .downField("data")
          .get[String]("emailAddress") must beRight("jim@acme.com")) and
          (contextJSecondElement
            .downField("data")
            .downField("data")
            .get[String]("emailAddress2") must beRight(
            "1c6660411341411d5431669699149283d10e070224be4339d52bbc4b007e78c5"
          )) and
          (contextJSecondElement.downField("data").get[String]("schema") must beRight(
            "iglu:com.acme/email_sent/jsonschema/1-0-0"
          )) and
          (unstructEventJ.get[String]("ip") must beRight(
            "269c433d0cc00395e3bc5fe7f06c5ad822096a38bec2d8a005367b52c0dfb428"
          )) and
          (unstructEventJ.get[String]("myVar2") must beRight("awesome"))

        first and second
    }

    size and validOut
  }

  def e3 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1),
            jsonPath = "$.field.that.does.not.exist.in.this.instance"
          )
        ),
        false,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )

    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "john@acme.com"
    expected.user_ipaddress = "70.46.123.145"
    expected.ip_domain = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor.downField("data")
        val firstElem = contextJ.downArray.downField("data")
        val secondElem = contextJ.downArray.right.downField("data")
        (firstElem.get[String]("emailAddress") must beRight("jim@acme.com")) and
          (firstElem.get[String]("emailAddress2") must beRight("bob@acme.com")) and
          (secondElem.get[String]("emailAddress") must beRight("tim@acme.com")) and
          (secondElem.get[String]("emailAddress2") must beRight("tom@acme.com"))
    }
    size and validOut
  }

  def e4 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0),
            // Last case throws an exeption if misconfigured
            jsonPath = "$.['emailAddress', 'emailAddress2', 'emailAddressNonExistent']"
          )
        ),
        false,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )

    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "john@acme.com"
    expected.user_ipaddress = "70.46.123.145"
    expected.ip_domain = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor.downField("data")
        val firstElem = contextJ.downArray.downField("data")
        val secondElem = contextJ.downArray.right.downField("data")
        (firstElem.get[String]("emailAddress") must beRight(
          "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
        )) and
          (firstElem.get[String]("emailAddress2") must beRight(
            "1c6660411341411d5431669699149283d10e070224be4339d52bbc4b007e78c5"
          )) and
          (secondElem.get[String]("emailAddress") must beRight("tim@acme.com")) and
          (secondElem.get[String]("emailAddress2") must beRight("tom@acme.com"))
    }
    size and validOut
  }

  def e5 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            fieldMutator = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1.some, None, 0.some),
            jsonPath = "$.emailAddress"
          )
        ),
        false,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )
    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "john@acme.com"
    expected.user_ipaddress = "70.46.123.145"
    expected.ip_domain = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor.downField("data")
        val firstElem = contextJ.downArray.downField("data")
        val secondElem = contextJ.downArray.right.downField("data")
        (firstElem.get[String]("emailAddress") must beRight(
          "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
        )) and
          (firstElem.get[String]("emailAddress2") must beRight("bob@acme.com")) and
          (secondElem.get[String]("emailAddress") must beRight(
            "09e4160b10703767dcb28d834c1905a182af0f828d6d3512dd07d466c283c840"
          )) and
          (secondElem.get[String]("emailAddress2") must beRight("tom@acme.com"))
    }
    size and validOut
  }

  def e6 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            fieldMutator = JsonMutators.get("contexts").get,
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1),
            jsonPath = "$.someInt"
          )
        ),
        false,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )
    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "john@acme.com"
    expected.user_ipaddress = "70.46.123.145"
    expected.ip_domain = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor.downField("data")
        val firstElem = contextJ.downArray.downField("data")
        val secondElem = contextJ.downArray.right.downField("data")
        (firstElem.get[String]("emailAddress") must beRight("jim@acme.com")) and
          (firstElem.get[String]("emailAddress2") must beRight("bob@acme.com")) and
          (secondElem.get[String]("emailAddress") must beRight("tim@acme.com")) and
          (secondElem.get[String]("emailAddress2") must beRight("tom@acme.com")) and
          (secondElem.get[Int]("someInt") must beRight(1))
    }
    size and validOut
  }

  def e7 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiScalar(fieldMutator = ScalarMutators("user_id")),
          PiiScalar(fieldMutator = ScalarMutators("user_ipaddress")),
          PiiScalar(fieldMutator = ScalarMutators("ip_domain")),
          PiiScalar(fieldMutator = ScalarMutators("user_fingerprint")),
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0),
            jsonPath = "$.emailAddress"
          ),
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 1, 0),
            jsonPath = "$.data.emailAddress2"
          ),
          PiiJson(
            fieldMutator = JsonMutators("unstruct_event"),
            schemaCriterion = SchemaCriterion("com.mailgun", "message_clicked", "jsonschema", 1, 0, 0),
            jsonPath = "$.ip"
          )
        ),
        true,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )
    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.ip_domain = null
    expected.geo_city = null
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    expected.pii =
      """{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/pii_transformation/jsonschema/1-0-0","data":{"pii":{"pojo":[{"fieldName":"user_fingerprint","originalValue":"its_you_again!","modifiedValue":"27abac60dff12792c6088b8d00ce7f25c86b396b8c3740480cd18e21068ecff4"},{"fieldName":"user_ipaddress","originalValue":"70.46.123.145","modifiedValue":"dd9720903c89ae891ed5c74bb7a9f2f90f6487927ac99afe73b096ad0287f3f5"},{"fieldName":"user_id","originalValue":"john@acme.com","modifiedValue":"7d8a4beae5bc9d314600667d2f410918f9af265017a6ade99f60a9c8f3aac6e9"}],"json":[{"fieldName":"unstruct_event","originalValue":"50.56.129.169","modifiedValue":"269c433d0cc00395e3bc5fe7f06c5ad822096a38bec2d8a005367b52c0dfb428","jsonPath":"$.ip","schema":"iglu:com.mailgun/message_clicked/jsonschema/1-0-0"},{"fieldName":"contexts","originalValue":"bob@acme.com","modifiedValue":"1c6660411341411d5431669699149283d10e070224be4339d52bbc4b007e78c5","jsonPath":"$.data.emailAddress2","schema":"iglu:com.acme/email_sent/jsonschema/1-1-0"},{"fieldName":"contexts","originalValue":"jim@acme.com","modifiedValue":"72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6","jsonPath":"$.emailAddress","schema":"iglu:com.acme/email_sent/jsonschema/1-0-0"}]},"strategy":{"pseudonymize":{"hashFunction":"SHA-256"}}}}}"""

    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor.downField("data")
        val firstElem = contextJ.downArray.downField("data")
        val secondElem = contextJ.downArray.right.downField("data")
        val unstructEventJ =
          parse(enrichedEvent.unstruct_event).toOption.get.hcursor.downField("data")

        (enrichedEvent.pii must_== expected.pii) and // This is the important test, the rest just verify that nothing has changed.
          (enrichedEvent.app_id must_== expected.app_id) and
          (enrichedEvent.ip_domain must_== expected.ip_domain) and
          (enrichedEvent.geo_city must_== expected.geo_city) and
          (enrichedEvent.etl_tstamp must_== expected.etl_tstamp) and
          (enrichedEvent.collector_tstamp must_== expected.collector_tstamp) and
          (firstElem.get[String]("emailAddress2") must beRight("bob@acme.com")) and
          (secondElem.get[String]("emailAddress") must beRight("tim@acme.com")) and
          (secondElem.get[String]("emailAddress2") must beRight("tom@acme.com")) and
          (secondElem
            .downField("data")
            .get[String]("emailAddress") must beRight("jim@acme.com")) and
          (secondElem.get[String]("schema") must beRight(
            "iglu:com.acme/email_sent/jsonschema/1-0-0"
          )) and
          (unstructEventJ.downField("data").get[String]("myVar2") must beRight("awesome"))
    }
    size and validOut
  }

  def e8 = {
    val enrichmentReg = EnrichmentRegistry[Id](
      ipLookups = ipEnrichment.some,
      piiPseudonymizer = PiiPseudonymizerEnrichment(
        List(
          PiiJson(
            fieldMutator = JsonMutators("contexts"),
            schemaCriterion = SchemaCriterion("com.acme", "email_sent", "jsonschema", 1, 0, 0),
            jsonPath = "$.['emailAddress', 'nonExistentEmailAddress']"
          )
        ),
        true,
        PiiStrategyPseudonymize(
          "SHA-256",
          hashFunction = DigestUtils.sha256Hex(_: Array[Byte]),
          "pepper123"
        )
      ).some
    )
    val output = commonSetup(enrichmentReg)
    val expected = new EnrichedEvent()
    expected.app_id = "ads"
    expected.user_id = "john@acme.com"
    expected.user_ipaddress = "70.46.123.145"
    expected.ip_domain = null
    expected.user_fingerprint = "its_you_again!"
    expected.geo_city = "Delray Beach"
    expected.etl_tstamp = "1970-01-18 08:40:00.000"
    expected.collector_tstamp = "2017-07-14 03:39:39.000"
    val size = output.size must_== 1
    val validOut = output.head must beValid.like {
      case enrichedEvent =>
        val contextJ = parse(enrichedEvent.contexts).toOption.get.hcursor.downField("data")
        val firstElem = contextJ.downArray.downField("data")
        val secondElem = contextJ.downArray.right.downField("data")

        (firstElem.get[String]("emailAddress") must beRight(
          "72f323d5359eabefc69836369e4cabc6257c43ab6419b05dfb2211d0e44284c6"
        )) and
          (firstElem.downField("data").get[String]("nonExistentEmailAddress") must beLeft) and
          (firstElem.get[String]("emailAddress2") must beRight("bob@acme.com")) and
          (secondElem.get[String]("emailAddress") must beRight("tim@acme.com")) and
          (secondElem.get[String]("emailAddress2") must beRight("tom@acme.com"))
    }
    size and validOut
  }
}
