/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
import java.lang.{Byte => JByte}

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.jackson.JsonMethods.parse

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey

// Scala-Forex
import com.snowplowanalytics.forex.oerclient.DeveloperAccount

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

/**
 * Tests enrichmentConfigs
 */
class EnrichmentConfigsSpec extends Specification with ValidationMatchers {

  "Parsing a valid anon_ip enrichment JSON" should {
    "successfully construct an AnonIpEnrichment case class" in {

      val ipAnonJson = parse("""{
        "enabled": true,
        "parameters": {
          "anonOctets": 2
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "anon_ip", "jsonschema", "1-0-0")

      val result = AnonIpEnrichment.parse(ipAnonJson, schemaKey)
      result must beSuccessful(AnonIpEnrichment(AnonOctets(2)))

    }
  }

  "Parsing a valid ip_lookups enrichment JSON" should {
    "successfully construct a GeoIpEnrichment case class" in {

      val ipToGeoJson = parse("""{
        "enabled": true,
        "parameters": {
          "geo": {
            "database": "GeoIPCity.dat",
            "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
          },
          "isp": {
            "database": "GeoIPISP.dat",
            "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"            
          }
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_lookups", "jsonschema", "1-0-0")

      val expected = IpLookupsEnrichment(Some("geo", new URI("http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIPCity.dat"), "GeoIPCity.dat"),
                                         Some("isp", new URI("http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoIPISP.dat"), "GeoIPISP.dat"),
                                         None, None, None, true)

      val result = IpLookupsEnrichment.parse(ipToGeoJson, schemaKey, true)
      result must beSuccessful(expected)

    }
  }

  "Parsing a valid referer_parser enrichment JSON" should {
    "successfully construct a RefererParserEnrichment case class" in {

      val refererParserJson = parse("""{
        "enabled": true,
        "parameters": {
          "internalDomains": [
            "www.subdomain1.snowplowanalytics.com", 
            "www.subdomain2.snowplowanalytics.com"
          ]
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "referer_parser", "jsonschema", "1-0-0")

      val expected = RefererParserEnrichment(List("www.subdomain1.snowplowanalytics.com", "www.subdomain2.snowplowanalytics.com"))

      val result = RefererParserEnrichment.parse(refererParserJson, schemaKey)
      result must beSuccessful(expected)

    }      
  }

  "Parsing a valid campaign_attribution enrichment JSON" should {
    "successfully construct a CampaignAttributionEnrichment case class" in {

      val campaignAttributionEnrichmentJson = parse("""{
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
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "campaign_attribution", "jsonschema", "1-0-0")

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

      val  userAgentUtilsEnrichmentJson = parse("""{
        "enabled": true,
        "parameters": {
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "user_agent_utils_config", "jsonschema", "1-0-0")

      val result = UserAgentUtilsEnrichmentConfig.parse(userAgentUtilsEnrichmentJson, schemaKey)
      result must beSuccessful(UserAgentUtilsEnrichment)

    }
  }

    "Parsing a valid ua_parser_config enrichment JSON" should {
    "successfully construct a UaParserEnrichment case object" in {

      val  uaParserEnrichmentJson = parse("""{
        "enabled": true,
        "parameters": {
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ua_parser_config", "jsonschema", "1-0-0")

      val result = UaParserEnrichmentConfig.parse(uaParserEnrichmentJson, schemaKey)
      result must beSuccessful(UaParserEnrichment)

    }
  }

  "Parsing a valid currency_convert_config enrichment JSON" should {
    "successfully construct a CurrencyConversionEnrichment case object" in {

      val  currencyConversionEnrichmentJson = parse("""{
        "enabled": true,
        "parameters": {
          "accountType": "DEVELOPER",
          "apiKey": "---",
          "baseCurrency": "EUR",
          "rateAt": "EOD_PRIOR"
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "currency_conversion_config", "jsonschema", "1-0-0")

      val result = CurrencyConversionEnrichmentConfig.parse(currencyConversionEnrichmentJson, schemaKey)
      result must beSuccessful(CurrencyConversionEnrichment(DeveloperAccount, "---", "EUR", "EOD_PRIOR"))

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
            "script": "${encoded}"
          }
        }""")
      }

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", "1-0-0")

      // val expected = JavascriptScriptEnrichment(JavascriptScriptEnrichmentConfig.compile(script).toOption.get)

      val result = JavascriptScriptEnrichmentConfig.parse(javascriptScriptEnrichmentJson, schemaKey)
      result must beSuccessful // TODO: check the result's contents by evaluating some JavaScript
    }
  }


  "Parsing a valid referer_parser enrichment JSON" should {
    "successfully construct a RefererParserEnrichment case class" in {

      val refererParserJson = parse("""{
        "enabled": true,
        "parameters": {
          "hashAlgorithm": "MD5",
          "excludeParameters": ["stm"]
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "event_fingerprint_config", "jsonschema", "1-0-0")

      val expectedExcludedParameters = List("stm")

      val result = EventFingerprintEnrichmentConfig.parse(refererParserJson, schemaKey)
      result must beSuccessful

      result.foreach {
        _.algorithm("sample") must_== "5e8ff9bf55ba3508199d22e984129be6"
      }

    }
  }
}
