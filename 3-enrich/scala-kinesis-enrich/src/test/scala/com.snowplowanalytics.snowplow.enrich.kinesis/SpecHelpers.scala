/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package enrich
package kinesis

// Java
import java.util.regex.Pattern

// Scalaz
import scalaz._
import Scalaz._

// Config
import com.typesafe.config.{ConfigFactory,Config,ConfigException}

// Specs2
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.matcher.Matchers._

// json4s
import org.json4s.jackson.JsonMethods._

// Iglu
import com.snowplowanalytics.iglu.client.Resolver

// Snowplow
import sources.TestSource
import common.outputs.EnrichedEvent
import common.utils.JsonUtils
import common.enrichments.EnrichmentRegistry

/**
 * Defines some useful helpers for the specs.
 */
object SpecHelpers {

  /**
   * The Kinesis Enrich being used
   */
  val EnrichVersion = "kinesis-0.4.0-common-0.13.0"

  val TimestampRegex = "[0-9]+"

  /**
   * The regexp pattern for a Type 4 UUID.
   *
   * Taken from Gajus Kuizinas's SO answer:
   * http://stackoverflow.com/a/14166194/255627
   *
   * TODO: should this be a Specs2 contrib?
   */
  val Uuid4Regexp = "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}"

  /**
   * Fields in our EnrichedEvent which will be checked
   * against a regexp, not for equality.
   */
  private val UseRegexpFields = List("event_id", "etl_tstamp")

  /**
   * The names of the fields written out
   */
  lazy val OutputFields = classOf[EnrichedEvent]
      .getDeclaredFields
      .map(_.getName)

  /**
   * User-friendly wrapper to instantiate
   * a BeFieldEqualTo Matcher.
   */
  def beFieldEqualTo(expected: String, withIndex: Int) = new BeFieldEqualTo(expected, withIndex)

  /**
   * A Specs2 matcher to check if a EnrichedEvent
   * field is correctly set.
   *
   * A couple of neat tricks:
   *
   * 1. Applies a regexp comparison if the field is
   *    only regexpable, not equality-comparable
   * 2. On failure, print out the field's name as
   *    well as the mismatch, to help with debugging
   */
  class BeFieldEqualTo(expected: String, index: Int) extends Matcher[String] {

    private val field = OutputFields(index)
    private val regexp = useRegexp(field)

    def apply[S <: String](actual: Expectable[S]) = {

      lazy val successMsg = s"$field: ${actual.description} %s $expected".format(
        if (regexp) "matches" else "equals")

      lazy val failureMsg = s"$field: ${actual.description} does not %s $expected".format(
        if (regexp) "match" else "equal")

      result(equalsOrMatches(regexp, actual.value, expected),
        successMsg, failureMsg, actual)
    }

    /**
     * Checks that the fields equal each other,
     * or matches the regular expression as
     * required.
     *
     * @param useRegexp Whether we should do an
     * equality check or a regexp match
     * @param actual The actual value
     * @param expected The expected value, or
     * regular expression to match against
     * @return true if the actual equals or
     * matches expected, false otherwise
     */
    private def equalsOrMatches(useRegexp: Boolean, actual: String, expected: String): Boolean = {

      if (useRegexp) {
        val pattern = Pattern.compile(expected)
        pattern.matcher(actual).matches
      } else {
        actual == expected
      }
    }

    /**
     * Whether a field in EnrichedEvent needs
     * a regexp-based comparison.
     *
     * @param field The name of the field
     * @return true if the field is regexpable,
     *         false otherwise
     */
    private def useRegexp(field: String): Boolean =
      UseRegexpFields.contains(field)
  }

  /**
   * A TestSource for testing against.
   * Built using an inline configuration file
   * with both source and sink set to test.
   */
  lazy val TestSource = {

    val enrichmentConfig = """|{
      |"schema": "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0",
      |"data": [
        |{
          |"schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
          |"data": {
            |"vendor": "com.snowplowanalytics.snowplow",
            |"name": "anon_ip",
            |"enabled": true,
            |"parameters": {
              |"anonOctets": 1
            |}
          |}
        |},
        |{
          |"schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/1-0-0",
          |"data": {
            |"vendor": "com.snowplowanalytics.snowplow",
            |"name": "ip_lookups",
            |"enabled": true,
            |"parameters": {
              |"geo": {
                |"database": "GeoIPCity.dat",
                |"uri":  "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
              |}
            |}
          |}  
        |},
        |{
          |"schema": "iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-0",
          |"data": {
            |"vendor": "com.snowplowanalytics.snowplow",
            |"name": "campaign_attribution",
            |"enabled": true,
            |"parameters": {
              |"mapping": "static",
              |"fields": {
                |"mktMedium": ["utm_medium", "medium"],
                |"mktSource": ["utm_source", "source"],
                |"mktTerm": ["utm_term", "legacy_term"],
                |"mktContent": ["utm_content"],
                |"mktCampaign": ["utm_campaign", "cid", "legacy_campaign"]
              |}
            |}
          |}
        |},
        |{
          |"schema": "iglu:com.snowplowanalytics.snowplow/user_agent_utils_config/jsonschema/1-0-0",
          |"data": {
            |"vendor": "com.snowplowanalytics.snowplow",
            |"name": "user_agent_utils_config",
            |"enabled": true,
            |"parameters": {
            |}
          |}
        |},
        |{
          |"schema": "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/1-0-0",
          |"data": {
            |"vendor": "com.snowplowanalytics.snowplow",
            |"name": "referer_parser",
            |"enabled": true,
            |"parameters": {
              |"internalDomains": ["www.subdomain1.snowplowanalytics.com"]
            |}
          |}  
        |}              
      |]
    |}""".stripMargin.replaceAll("[\n\r]","").stripMargin.replaceAll("[\n\r]","")

    val config = """
enrich {
  source = "test"
  sink= "test"

  aws {
    access-key: "cpf"
    secret-key: "cpf"
  }

  streams {
    in: {
      raw: "SnowplowRaw"
    }
    out: {
      enriched: "SnowplowEnriched"
      enriched_shards: 1 # Number of shards to use if created.
      bad: "SnowplowBad" # Not used until #463
      bad_shards: 1 # Number of shards to use if created.
    }
    app-name: SnowplowKinesisEnrich-${enrich.streams.in.raw}
    initial-position = "TRIM_HORIZON"
    region: "us-east-1"
  }
  enrichments {
    geo_ip: {
      enabled: true # false not yet suported
      maxmind_file: "/maxmind/GeoLiteCity.dat" # SBT auto-downloads into resource_managed/test
    }
    anon_ip: {
      enabled: true
      anon_octets: 1 # Or 2, 3 or 4. 0 is same as enabled: false
    }
  }
}
"""

    val validatedResolver = for {
      json <- JsonUtils.extractJson("", """{
        "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
        "data": {

          "cacheSize": 500,
          "repositories": [
            {
              "name": "Iglu Central",
              "priority": 0,
              "vendorPrefixes": [ "com.snowplowanalytics" ],
              "connection": {
                "http": {
                  "uri": "http://iglucentral.com"
                }
              }
            }
          ]
        }
      }
      """)
      resolver <- Resolver.parse(json).leftMap(_.toString)
    } yield resolver

    implicit val resolver: Resolver = validatedResolver.fold(
      e => throw new RuntimeException(e),
      s => s
    )

    val enrichmentRegistry = (for {
      registryConfig <- JsonUtils.extractJson("", enrichmentConfig)
      reg <- EnrichmentRegistry.parse(fromJsonNode(registryConfig), true).leftMap(_.toString)
    } yield reg) fold (
      e => throw new RuntimeException(e),
      s => s
    )

    val conf = ConfigFactory.parseString(config)
    val kec = new KinesisEnrichConfig(conf)

    new TestSource(kec, resolver, enrichmentRegistry)
  }
}
