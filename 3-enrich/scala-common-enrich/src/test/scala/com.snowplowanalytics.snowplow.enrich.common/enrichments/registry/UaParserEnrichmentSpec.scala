/**Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.specs2.matcher.DataTables
import org.specs2.scalaz._

class UaParserEnrichmentSpec extends org.specs2.mutable.Specification with ValidationMatchers with DataTables {

  val mobileSafariUserAgent =
    "Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3"
  val mobileSafariJson =
    """{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Mobile Safari","useragentMajor":"5","useragentMinor":"1","useragentPatch":null,"useragentVersion":"Mobile Safari 5.1","osFamily":"iOS","osMajor":"5","osMinor":"1","osPatch":"1","osPatchMinor":null,"osVersion":"iOS 5.1.1","deviceFamily":"iPhone"}}"""
  val safariUserAgent =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10) AppleWebKit/600.1.25 (KHTML, like Gecko) Version/8.0 Safari/600.1.25"
  val safariJson =
    """{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Safari","useragentMajor":"8","useragentMinor":"0","useragentPatch":null,"useragentVersion":"Safari 8.0","osFamily":"Mac OS X","osMajor":"10","osMinor":"10","osPatch":null,"osPatchMinor":null,"osVersion":"Mac OS X 10.10","deviceFamily":"Other"}}"""

  // The URI is irrelevant here, but the local file name needs to point to our test resource
  val testRulefile = getClass.getResource("uap-test-rules.yml").toURI.getPath
  val customRules  = (new URI("s3://private-bucket/files/uap-rules.yml"), testRulefile)
  val testAgentJson =
    """{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"UAP Test Family","useragentMajor":null,"useragentMinor":null,"useragentPatch":null,"useragentVersion":"UAP Test Family","osFamily":"UAP Test OS","osMajor":null,"osMinor":null,"osPatch":null,"osPatchMinor":null,"osVersion":"UAP Test OS","deviceFamily":"UAP Test Device"}}"""

  "useragent parser enrichment" should {
    "report files needing to be cached" in {
      "Custom Rules"      | "Cached Files" |
        None              !! List.empty |
        Some(customRules) !! List(customRules) |> { (rules, cachedFiles) =>
        {
          UaParserEnrichment(rules).filesToCache must_== cachedFiles
        }
      }
    }
  }

  "useragent parser" should {
    "parse useragent according to configured rules" in {
      "Custom Rules"      | "Input UserAgent"      | "Parsed UserAgent" |
        None              !! mobileSafariUserAgent !! mobileSafariJson |
        None              !! safariUserAgent       !! safariJson |
        Some(customRules) !! mobileSafariUserAgent !! testAgentJson |> { (rules, input, expected) =>
        {
          UaParserEnrichment(rules).extractUserAgent(input) must beSuccessful.like {
            case a => compact(render(a)) must_== compact(render(parse(expected)))
          }
        }
      }
    }
  }

  val badRulefile = (new URI("s3://private-bucket/files/uap-rules.yml"), "NotAFile")

  "useragent parser" should {
    "report initialization error" in {
      "Custom Rules"      | "Input UserAgent"      | "Parsed UserAgent" |
        Some(badRulefile) !! mobileSafariUserAgent !! "Failed to initialize ua parser" |> {
        (rules, input, errorPrefix) =>
          {
            UaParserEnrichment(rules).extractUserAgent(input) must beFailing.like {
              case a => a must startWith(errorPrefix)
            }
          }
      }
    }
  }
}
