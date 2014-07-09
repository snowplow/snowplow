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

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.jackson.JsonMethods.parse

// Iglu
import com.snowplowanalytics.iglu.client.SchemaKey

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

/**
 * Tests enrichmentConfigs
 */
class EnrichmentConfigsTest extends Specification with ValidationMatchers {

  "Parsing a valid anon_ip enrichment JSON" should {
    "successfully construct an AnonIpEnrichmentConfig case class" in {

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

  "Parsing a valid ip_to_geo enrichment JSON" should {
    "successfully construct a GeoIpEnrichmentConfig case class" in {

      val ipToGeoJson = parse("""{
        "enabled": true,
        "parameters": {
          "geo": {
            "database": "GeoLiteCity.dat",
            "uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
          }
        }
      }""")

      val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "ip_to_geo", "jsonschema", "1-0-0")

      val expected = IpLookupsEnrichment(Some(new URI("http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/GeoLiteCity.dat"), "GeoLiteCity.dat"), None, None, None, true)

      val result = IpLookupsEnrichment.parse(ipToGeoJson, schemaKey, true)
      result must beSuccessful(expected)

    }
  }

  "Parsing a valid referer_parser enrichment JSON" should {
    "successfully construct a GeoIpEnrichmentConfig case class" in {

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

  // TODO: a test in HDFS mode too?

}
