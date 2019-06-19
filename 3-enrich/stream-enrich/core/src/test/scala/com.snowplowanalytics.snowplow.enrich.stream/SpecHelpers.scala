/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.stream

import java.util.regex.Pattern

import scala.util.matching.Regex

import cats.Id
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.snowplow.enrich.common.adapters.AdapterRegistry
import com.snowplowanalytics.snowplow.enrich.common.adapters.registry.RemoteAdapter
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.JsonUtils
import org.specs2.matcher.{Expectable, Matcher}

import sources.TestSource
import utils._

/**
 * Defines some useful helpers for the specs.
 */
object SpecHelpers {

  implicit def stringToJustString(s: String) = JustString(s)
  implicit def regexToJustRegex(r: Regex) = JustRegex(r)

  /**
   * The Stream Enrich being used
   */
  val EnrichVersion =
    s"stream-enrich-${generated.BuildInfo.version}-common-${generated.BuildInfo.commonEnrichVersion}"

  val TimestampRegex =
    "[0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\\.\\d{3})?".r

  /**
   * The regexp pattern for a Type 4 UUID.
   *
   * Taken from Gajus Kuizinas's SO answer:
   * http://stackoverflow.com/a/14166194/255627
   *
   * TODO: should this be a Specs2 contrib?
   */
  val Uuid4Regexp = "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}".r

  val ContextWithUuid4Regexp =
    new Regex(
      Pattern.quote(
        """{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/parent_event/jsonschema/1-0-0","data":{"parentEventId":""""
      ) +
        "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}" +
        Pattern.quote("\"}}]}")
    )

  /**
   * The names of the fields written out
   */
  lazy val OutputFields = classOf[EnrichedEvent].getDeclaredFields
    .map(_.getName)

  /**
   * User-friendly wrapper to instantiate
   * a BeFieldEqualTo Matcher.
   */
  def beFieldEqualTo(expected: StringOrRegex, withIndex: Int) =
    new BeFieldEqualTo(expected, withIndex)

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
  class BeFieldEqualTo(expected: StringOrRegex, index: Int) extends Matcher[String] {

    private val field = OutputFields(index)

    private val regexp = expected match {
      case JustRegex(_) => true
      case JustString(_) => false
    }

    def apply[S <: String](actual: Expectable[S]) = {

      lazy val successMsg =
        s"$field: ${actual.description} %s $expected".format(if (regexp) "matches" else "equals")

      lazy val failureMsg =
        s"$field: ${actual.description} does not %s $expected"
          .format(if (regexp) "match" else "equal")

      result(equalsOrMatches(actual.value, expected), successMsg, failureMsg, actual)
    }

    /**
     * Checks that the fields equal each other,
     * or matches the regular expression as
     * required.
     *
     * @param actual The actual value
     * @param expected The expected value, or
     * regular expression to match against
     * @return true if the actual equals or
     * matches expected, false otherwise
     */
    private def equalsOrMatches(actual: String, expected: StringOrRegex): Boolean = expected match {
      case JustRegex(r) => r.pattern.matcher(actual).matches
      case JustString(s) => actual == s
    }
  }

  /**
   * A TestSource for testing against.
   * Built using an inline configuration file
   * with both source and sink set to test.
   */
  lazy val TestSource = new TestSource(client, adapterRegistry, enrichmentRegistry)
  val igluCentralDefaultConfig =
    """{
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
        },
        {
          "name": "referer 2.0",
          "priority": 0,
          "vendorPrefixes": [ "com.snowplowanalytics" ],
          "connection": {
            "http": {
              "uri": "http://iglucentral-dev.com.s3-website-us-east-1.amazonaws.com/referer-parser-2"
            }
          }
        }
      ]
    }
  }
  """

  val igluConfig = {
    val resolverEnvVar = for {
      config <- sys.env.get("ENRICH_RESOLVER_CONFIG")
      if config.nonEmpty
    } yield config
    resolverEnvVar.getOrElse(igluCentralDefaultConfig)
  }
  val validatedResolver = for {
    json <- JsonUtils.extractJson(igluConfig)
    resolver <- Client.parseDefault[Id](json).leftMap(_.toString).value
  } yield resolver

  val client = validatedResolver.fold(
    e => throw new RuntimeException(e),
    s => s
  )

  val enrichmentConfig =
    """|{
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
          |"schema": "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0",
          |"data": {
            |"vendor": "com.snowplowanalytics.snowplow",
            |"name": "referer_parser",
            |"enabled": true,
            |"parameters": {
              |"internalDomains": ["www.subdomain1.snowplowanalytics.com"],
              |"database": "referer-tests.json",
              |"uri": "http://snowplow.com"
            |}
          |}
        |},
        |{
        |"schema": "iglu:com.snowplowanalytics.snowplow.enrichments/pii_enrichment_config/jsonschema/2-0-0",
        |"data": {
          |"vendor": "com.snowplowanalytics.snowplow.enrichments",
          |"name": "pii_enrichment_config",
          |"emitEvent": true,
          |"enabled": true,
          |"parameters": {
            |"pii": [
              |{
                |"pojo": {
                  |"field": "user_id"
                |}
              |},
              |{
                |"pojo": {
                  |"field": "user_ipaddress"
                |}
              |},
              |{
                |"json": {
                  |"field": "unstruct_event",
                  |"schemaCriterion": "iglu:com.mailgun/message_delivered/jsonschema/1-0-*",
                  |"jsonPath": "$$['recipient']"
                |}
              |},
              |{
                |"json": {
                  |"field": "unstruct_event",
                  |"schemaCriterion": "iglu:com.mailchimp/subscribe/jsonschema/1-*-*",
                  |"jsonPath": "$$.data.['email', 'ip_opt']"
                |}
              |}
            |],
            |"strategy": {
              |"pseudonymize": {
                |"hashFunction": "SHA-1",
                |"salt": "pepper123"
              |}
            |}
          |}
        |}
      |}
    |]
  |}""".stripMargin.replaceAll("[\n\r]", "").stripMargin.replaceAll("[\n\r]", "")

  val enrichmentRegistry = (for {
    registryConfig <- JsonUtils.extractJson(enrichmentConfig)
    confs <- EnrichmentRegistry.parse(registryConfig, client, true).leftMap(_.toString).toEither
    reg <- EnrichmentRegistry.build[Id](confs).value
  } yield reg) fold (
    e => throw new RuntimeException(e),
    s => s
  )

  // Init AdapterRegistry with one RemoteAdapter used for integration tests
  val adapterRegistry = new AdapterRegistry(
    Map(("remoteVendor", "v42") -> new RemoteAdapter("http://localhost:9090/", None, None))
  )
}
