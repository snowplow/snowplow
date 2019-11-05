/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.Eval
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import io.circe.literal._
import org.joda.time.DateTime
import org.specs2.Specification
import org.specs2.matcher.DataTables

class IabEnrichmentSpec extends Specification with DataTables {

  def is = s2"""
  performCheck should correctly perform IAB checks on valid input $e1
  performCheck should fail on invalid IP                          $e2
  getIabContext should fail on missing fields                     $e3
  getIabContext should return a valid JObject on valid input      $e4
  """

  // When testing, localMode is set to true, so the URIs are ignored and the databases are loaded from test/resources
  val validConfig = IabEnrichment
    .parse(
      json"""{
      "name": "iab_spiders_and_robots_enrichment",
      "vendor": "com.snowplowanalytics.snowplow.enrichments",
      "enabled": false,
      "parameters": {
        "ipFile": {
          "database": "ip_exclude_current_cidr.txt",
          "uri": "s3://my-private-bucket/iab"
        },
        "excludeUseragentFile": {
          "database": "exclude_current.txt",
          "uri": "s3://my-private-bucket/iab"
        },
        "includeUseragentFile": {
          "database": "include_current.txt",
          "uri": "s3://my-private-bucket/iab"
        }
      }
    }""",
      SchemaKey(
        "com.snowplowanalytics.snowplow.enrichments",
        "iab_spiders_and_robots_enrichment",
        "jsonschema",
        SchemaVer.Full(1, 0, 0)
      ),
      true
    )
    .toOption
    .get

  def e1 =
    "SPEC NAME" || "USER AGENT" | "IP ADDRESS" | "EXPECTED SPIDER OR ROBOT" | "EXPECTED CATEGORY" | "EXPECTED REASON" | "EXPECTED PRIMARY IMPACT" |
      "null UA/IP" !! null ! null ! false ! "BROWSER" ! "PASSED_ALL" ! "NONE" |
      "valid UA/IP" !! "Xdroid" ! "192.168.0.1" ! false ! "BROWSER" ! "PASSED_ALL" ! "NONE" |
      "valid UA, excluded IP" !! "Mozilla/5.0" ! "192.168.151.21" ! true ! "SPIDER_OR_ROBOT" ! "FAILED_IP_EXCLUDE" ! "UNKNOWN" |
      "invalid UA, excluded IP" !! "xonitor" ! "192.168.0.1" ! true ! "SPIDER_OR_ROBOT" ! "FAILED_UA_INCLUDE" ! "UNKNOWN" |> {
      (
        _,
        userAgent,
        ipAddress,
        expectedSpiderOrRobot,
        expectedCategory,
        expectedReason,
        expectedPrimaryImpact
      ) =>
        (for {
          e <- validConfig.enrichment[Eval]
          res = e.performCheck(userAgent, ipAddress, DateTime.now())
        } yield res).value must beRight.like {
          case check =>
            check.spiderOrRobot must_== expectedSpiderOrRobot and
              (check.category must_== expectedCategory) and
              (check.reason must_== expectedReason) and
              (check.primaryImpact must_== expectedPrimaryImpact)
        }
    }

  def e2 =
    validConfig.enrichment[Eval].map(_.performCheck("", "foo//bar", DateTime.now())).value must
      beLeft

  def e3 =
    validConfig.enrichment[Eval].map(_.getIabContext(None, None, None)).value must beLeft

  def e4 = {
    val responseJson = json"""
      {
          "schema": "iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0",
          "data": {
              "spiderOrRobot": false,
              "category": "BROWSER",
              "reason": "PASSED_ALL",
              "primaryImpact": "NONE"
          }
      }
    """
    validConfig
      .enrichment[Eval]
      .map(_.getIabContext(Some("Xdroid"), Some("192.168.0.1"), Some(DateTime.now())))
      .value must
      beRight(responseJson)
  }

}
