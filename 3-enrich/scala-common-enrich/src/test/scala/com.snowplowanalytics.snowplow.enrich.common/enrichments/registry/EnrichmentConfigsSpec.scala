/*
 * Copyright (c) 2014-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.common.enrichments.registry

import java.net.URI

import cats.syntax.either._
import com.snowplowanalytics.forex.oerclient.DeveloperAccount
import com.snowplowanalytics.iglu.client.SchemaKey
import io.circe.literal._
import io.circe.parser._
import org.apache.commons.codec.binary.Base64
import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

class EnrichmentConfigsSpec extends Specification with ValidationMatchers with DataTables {

  "Parsing a valid anon_ip enrichment JSON" should {
    "successfully construct an AnonIpEnrichment case class with default value for IPv6" in {
      val ipAnonJson = json"""{
        "enabled": true,
        "parameters": {
          "anonOctets": 2
        }
      }"""
      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", "1-0-0")
      val result = AnonIpEnrichment.parse(ipAnonJson, schemaKey)
      result must beSuccessful(AnonIpEnrichment(AnonIPv4Octets(2), AnonIPv6Segments(2)))

    }
  }

  "Parsing a valid anon_ip enrichment JSON" should {
    "successfully construct an AnonIpEnrichment case class" in {

      val ipAnonJson = parse("""{
        "enabled": true,
        "parameters": {
          "anonOctets": 2,
          "anonSegments": 3
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", "1-0-1")

      val result = AnonIpEnrichment.parse(ipAnonJson, schemaKey)
      result must beSuccessful(AnonIpEnrichment(AnonIPv4Octets(2), AnonIPv6Segments(3)))

    }
  }

  "Parsing a valid ip_lookups enrichment JSON" should {
    "successfully construct a GeoIpEnrichment case class" in {
      val ipToGeoJson = json"""{
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
      val schemaKey =
        SchemaKey("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", "2-0-0")
      val expected = IpLookupsEnrichment(
        Some((
          "geo",
          new URI(
            "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIP2-City.mmdb"),
          "GeoIP2-City.mmdb")),
        Some(
          (
            "isp",
            new URI(
              "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIP2-ISP.mmdb"),
            "GeoIP2-ISP.mmdb")),
        None,
        None,
        true
      )

      val result = IpLookupsEnrichment.parse(ipToGeoJson, schemaKey, true)
      result must beSuccessful(expected)

    }
  }

  "Parsing a valid referer_parser enrichment JSON" should {
    "successfully construct a RefererParserEnrichment case class" in {
      val refererParserJson = json"""{
        "enabled": true,
        "parameters": {
          "internalDomains": [
            "www.subdomain1.snowplowanalytics.com",
            "www.subdomain2.snowplowanalytics.com"
          ]
        }
      }"""
      val schemaKey =
        SchemaKey("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", "1-0-0")
      val expected =
        RefererParserEnrichment(
          List("www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com"))
      val result = RefererParserEnrichment.parse(refererParserJson, schemaKey)
      result must beSuccessful(expected)

    }
  }

  "Parsing a valid campaign_attribution enrichment JSON" should {
    "successfully construct a CampaignAttributionEnrichment case class" in {
      val campaignAttributionEnrichmentJson =
        parse(
          """{
          "enabled": true,
          "parameters": {
            "mapping": "static",
            "fields": {
              "mktMedium": ["utm_medium", "medium"],
              "mktSource": ["utm_source", "source"],
              "mktTerm": ["utm_term"],
              "mktContent": [],
              "mktCampaign": ["utm _ campaign", "CID", "legacy-campaign!?-`@#$%^&*()=\\][}{/.,<>~|"],
              "mktClickId": {
                "customclid": "Custom",
                "gclid": "Override"
              }
            }
          }
        }""").toOption.get
      val schemaKey =
        SchemaKey("com.snowplowanalytics.snowplow", "campaign_attribution", "jsonschema", "1-0-0")
      val expected = CampaignAttributionEnrichment(
        List("utm_medium", "medium"),
        List("utm_source", "source"),
        List("utm_term"),
        List(),
        List("utm _ campaign", "CID", "legacy-campaign!?-`@#$%^&*()=\\][}{/.,<>~|"),
        List(
          "gclid" -> "Override",
          "msclkid" -> "Microsoft",
          "dclid" -> "DoubleClick",
          "customclid" -> "Custom"
        )
      )
      val result = CampaignAttributionEnrichment.parse(campaignAttributionEnrichmentJson, schemaKey)
      result must beSuccessful(expected)
    }
  }

  "Parsing a valid user_agent_utils_config enrichment JSON" should {
    "successfully construct a UserAgentUtilsEnrichment case object" in {
      val userAgentUtilsEnrichmentJson = json"""{
        "enabled": true,
        "parameters": {
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "user_agent_utils_config",
        "jsonschema",
        "1-0-0")
      val result = UserAgentUtilsEnrichmentConfig.parse(userAgentUtilsEnrichmentJson, schemaKey)
      result must beSuccessful(UserAgentUtilsEnrichment)

    }
  }

  "Parsing a valid ua_parser_config enrichment JSON" should {
    "successfully construct a UaParserEnrichment case class" in {
      val schemaKey =
        SchemaKey("com.snowplowanalytics.snowplow", "ua_parser_config", "jsonschema", "1-0-1")
      val configWithDefaultRules = json"""{
        "enabled": true,
        "parameters": {
        }
      }"""
      val externalUri = "http://public-website.com/files/"
      val database = "myrules.yml"
      val configWithExternalRules = parse(raw"""{
        "enabled": true,
        "parameters": {
          "uri": "$externalUri",
          "database": "$database"
        }
      }""").toOption.get

      "Configuration" | "Custom Rules" |
        configWithDefaultRules !! None |
        configWithExternalRules !! Some((new URI(externalUri + database), "./ua-parser-rules.yml")) |> {
        (config, expected) =>
          {
            val result = UaParserEnrichmentConfig.parse(config, schemaKey)
            result must beSuccessful(UaParserEnrichment(expected))
          }
      }
    }
  }

  "Parsing a valid currency_convert_config enrichment JSON" should {
    "successfully construct a CurrencyConversionEnrichment case object" in {
      val currencyConversionEnrichmentJson = json"""{
        "enabled": true,
        "parameters": {
          "accountType": "DEVELOPER",
          "apiKey": "---",
          "baseCurrency": "EUR",
          "rateAt": "EOD_PRIOR"
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "currency_conversion_config",
        "jsonschema",
        "1-0-0")
      val result =
        CurrencyConversionEnrichmentConfig.parse(currencyConversionEnrichmentJson, schemaKey)
      result must beSuccessful(
        CurrencyConversionEnrichment(DeveloperAccount, "---", "EUR", "EOD_PRIOR"))
    }
  }

  "Parsing a valid javascript_script_config enrichment JSON" should {
    "successfully construct a JavascriptScriptEnrichment case class" in {
      val script =
        s"""|function process(event) {
            |  return [];
            |}
            |""".stripMargin
      val javascriptScriptEnrichmentJson = {
        val encoder = new Base64(true)
        val encoded = new String(encoder.encode(script.getBytes)).trim // Newline being appended by some Base64 versions
        parse(s"""{
          "enabled": true,
          "parameters": {
            "script": "$encoded"
          }
        }""").toOption.get
      }
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "javascript_script_config",
        "jsonschema",
        "1-0-0")
      val result = JavascriptScriptEnrichmentConfig.parse(javascriptScriptEnrichmentJson, schemaKey)
      result must beSuccessful // TODO: check the result's contents by evaluating some JavaScript
    }
  }

  "Parsing a valid event_fingerprint_config enrichment JSON" should {
    "successfully construct a EventFingerprintEnrichmentConfig case class" in {
      val refererParserJson = json"""{
        "enabled": true,
        "parameters": {
          "hashAlgorithm": "MD5",
          "excludeParameters": ["stm"]
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "event_fingerprint_config",
        "jsonschema",
        "1-0-0")
      val expectedExcludedParameters = List("stm")
      val result = EventFingerprintEnrichmentConfig.parse(refererParserJson, schemaKey)
      result must beSuccessful.like {
        case enr => enr.algorithm("sample") must beEqualTo("5e8ff9bf55ba3508199d22e984129be6")
      }
    }
  }

  "Parsing a valid cookie_extractor_config enrichment JSON" should {
    "successfully construct a CookieExtractorEnrichment case object" in {
      val cookieExtractorEnrichmentJson = json"""{
        "enabled": true,
        "parameters": {
          "cookies": ["foo", "bar"]
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow",
        "cookie_extractor_config",
        "jsonschema",
        "1-0-0")
      val result = CookieExtractorEnrichmentConfig.parse(cookieExtractorEnrichmentJson, schemaKey)
      result must beSuccessful(CookieExtractorEnrichment(List("foo", "bar")))
    }
  }

  "Parsing a valid pii_enrichment_config enrichment JSON" should {
    "successfully construct a PiiPsedonymizerEnrichment case object" in {
      import pii._
      val piiPseudonymizerEnrichmentJson =
        parse("""{
           "enabled": true,
           "emitEvent": true,
           "parameters": {
             "pii": [
               {
                 "pojo": {
                   "field": "user_id"
                 }
               },
               {
                 "json": {
                   "jsonPath": "$.emailAddress",
                   "schemaCriterion": "iglu:com.acme/email_sent/jsonschema/1-*-*",
                   "field": "contexts"
                 }
               }
             ],
             "strategy": {
               "pseudonymize": {
                 "hashFunction": "SHA-256",
                 "salt": "pepper"
               }
             }
           }
         }""").toOption.get
      val schemaKey =
        SchemaKey(
          "com.snowplowanalytics.snowplow.enrichments",
          "pii_enrichment_config",
          "jsonschema",
          "2-0-0")
      val result = PiiPseudonymizerEnrichment.parse(piiPseudonymizerEnrichmentJson, schemaKey)
      result must beSuccessful.like {
        case piiRes: PiiPseudonymizerEnrichment => {
          (piiRes.strategy must haveClass[PiiStrategyPseudonymize]) and
            (piiRes.strategy
              .asInstanceOf[PiiStrategyPseudonymize]
              .hashFunction("1234".getBytes("UTF-8"))
              must_== "03ac674216f3e15c761ee1a5e255f067953623c8b388b4459e13f978d7c846f4") and
            (piiRes.fieldList.size must_== 2) and
            (piiRes.fieldList(0) must haveClass[PiiScalar]) and
            (piiRes.fieldList(0).asInstanceOf[PiiScalar].fieldMutator must_== ScalarMutators
              .get("user_id")
              .get) and
            (piiRes.fieldList(1).asInstanceOf[PiiJson].fieldMutator must_== JsonMutators
              .get("contexts")
              .get) and
            (piiRes
              .fieldList(1)
              .asInstanceOf[PiiJson]
              .schemaCriterion
              .toString must_== "iglu:com.acme/email_sent/jsonschema/1-*-*") and
            (piiRes.fieldList(1).asInstanceOf[PiiJson].jsonPath must_== "$.emailAddress")
        }
      }
    }
  }

  "Parsing an iab_spiders_and_robots_enrichment JSON" should {
    "successfully construct an IabEnrichment case class" in {
      val iabJson = json"""{
        "enabled": true,
        "parameters": {
          "ipFile": {
            "database": "ip_exclude_current_cidr.txt",
            "uri": "https://example.com/"
          },
          "excludeUseragentFile": {
            "database": "exclude_current.txt",
            "uri": "https://example.com"
          },
          "includeUseragentFile": {
             "database": "include_current.txt",
             "uri": "https://example.com/"
          }
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "iab_spiders_and_robots_enrichment",
        "jsonschema",
        "1-0-0")
      val expected = IabEnrichment(
        Some(
          IabDatabase(
            "ipFile",
            new URI("https://example.com/ip_exclude_current_cidr.txt"),
            "ip_exclude_current_cidr.txt")),
        Some(
          IabDatabase(
            "excludeUseragentFile",
            new URI("https://example.com/exclude_current.txt"),
            "exclude_current.txt")),
        Some(
          IabDatabase(
            "includeUseragentFile",
            new URI("https://example.com/include_current.txt"),
            "include_current.txt")),
        true
      )
      val result = IabEnrichment.parse(iabJson, schemaKey, true)
      result must beSuccessful(expected)
    }

    "fail if a database file is missing" in {
      val iabJson = json"""{
        "enabled": true,
        "parameters": {
          "ipFile": {
            "database": "ip_exclude_current_cidr.txt",
            "uri": "https://example.com"
          },
          "excludeUseragentFile": {
            "database": "DOES_NOT_EXIST",
            "uri": "https://example.com"
          },
          "includeUseragentFile": {
             "database": "include_current.txt",
             "uri": "https://example.com"
          }
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "iab_spiders_and_robots_enrichment",
        "jsonschema",
        "1-0-0")
      IabEnrichment.parse(iabJson, schemaKey, true) must throwA[NullPointerException]

    }

    "fail if the URI to a database file is invalid" in {
      val iabJson = json"""{
        "enabled": true,
        "parameters": {
          "ipFile": {
            "database": "ip_exclude_current_cidr.txt",
            "uri": "https://example.com"
          },
          "excludeUseragentFile": {
            "database": "exclude_current.txt",
            "uri": "https://example.com"
          },
          "includeUseragentFile": {
             "database": "include_current.txt",
             "uri": "file://foo:{waaat}/"
          }
        }
      }"""
      val schemaKey = SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "iab_spiders_and_robots_enrichment",
        "jsonschema",
        "1-0-0")
      val result = IabEnrichment.parse(iabJson, schemaKey, true)
      result must beFailing
    }
  }
}
