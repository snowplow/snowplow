/**Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Eval
import cats.data.EitherT

import io.circe.literal._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

import org.specs2.matcher.DataTables
import org.specs2.mutable.Specification

class UaParserEnrichmentSpec extends Specification with DataTables {

  val mobileSafariUserAgent =
    "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3"
  val mobileSafariJson =
    SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ua_parser_context",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"useragentFamily":"Mobile Safari","useragentMajor":"5","useragentMinor":"1","useragentPatch":null,"useragentVersion":"Mobile Safari 5.1","osFamily":"iOS","osMajor":"5","osMinor":"1","osPatch":"1","osPatchMinor":null,"osVersion":"iOS 5.1.1","deviceFamily":"iPhone"}"""
    )

  val safariUserAgent =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25"
  val safariJson =
    SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ua_parser_context",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"useragentFamily":"Safari","useragentMajor":"8","useragentMinor":"0","useragentPatch":null,"useragentVersion":"Safari 8.0","osFamily":"Mac OS X","osMajor":"10","osMinor":"10","osPatch":null,"osPatchMinor":null,"osVersion":"Mac OS X 10.10","deviceFamily":"Other"}"""
    )

  // The URI is irrelevant here, but the local file name needs to point to our test resource
  val testRulefile = getClass.getResource("uap-test-rules.yml").toURI.getPath
  val customRules = (new URI("s3://private-bucket/files/uap-rules.yml"), testRulefile)
  val testAgentJson =
    SelfDescribingData(
      SchemaKey(
        "com.snowplowanalytics.snowplow",
        "ua_parser_context",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      json"""{"useragentFamily":"UAP Test Family","useragentMajor":null,"useragentMinor":null,"useragentPatch":null,"useragentVersion":"UAP Test Family","osFamily":"UAP Test OS","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"UAP Test OS","deviceFamily":"UAP Test Device"}"""
    )

  val schemaKey = SchemaKey("vendor", "name", "format", SchemaVer.Full(1, 0, 0))

  val badRulefile = (new URI("s3://private-bucket/files/uap-rules.yml"), "NotAFile")

  "useragent parser" should {
    "report initialization error" in {
      "Custom Rules" | "Input UserAgent" | "Parsed UserAgent" |
        Some(badRulefile) !! mobileSafariUserAgent !! "Failed to initialize ua parser" |> { (rules, input, errorPrefix) =>
        (for {
          c <- EitherT.rightT[Eval, String](UaParserConf(schemaKey, rules))
          e <- c.enrichment[Eval]
          res = e.extractUserAgent(input)
        } yield res).value.value must beLeft.like {
          case a => a must startWith(errorPrefix)
        }
      }
    }

    "parse useragent according to configured rules" in {
      "Custom Rules" | "Input UserAgent" | "Parsed UserAgent" |
        None !! mobileSafariUserAgent !! mobileSafariJson |
        None !! safariUserAgent !! safariJson |
        Some(customRules) !! mobileSafariUserAgent !! testAgentJson |> { (rules, input, expected) =>
        val json = for {
          c <- EitherT.rightT[Eval, String](UaParserConf(schemaKey, rules))
          e <- c.enrichment[Eval].leftMap(_.toString)
          res <- EitherT.fromEither[Eval](e.extractUserAgent(input)).leftMap(_.toString)
        } yield res
        json.value.value must beRight(expected)
      }
    }
  }
}
