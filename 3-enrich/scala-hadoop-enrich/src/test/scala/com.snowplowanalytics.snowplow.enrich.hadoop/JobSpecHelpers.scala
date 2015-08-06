/*
 * Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
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
package hadoop

// Java
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Scala
import scala.collection.mutable.ListBuffer

// Specs2
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.matcher.Matchers._

// Scalding
import com.twitter.scalding._

// Snowplow Common Enrich
import common.outputs.EnrichedEvent

// Scalaz
import scalaz._
import Scalaz._

/**
 * Holds helpers for running integration
 * tests on SnowPlow EtlJobs.
 */
object JobSpecHelpers {

  /**
   * The current version of our Hadoop ETL
   */
  val EtlVersion = "hadoop-1.0.0-common-0.15.0"

  val EtlTimestamp = "2001-09-09 01:46:40.000"

  /**
   * Fields in our CanonicalOutput which are unmatchable
   */
  private val UnmatchableFields = List("event_id")

  /**
   * The names of the fields written out
   */
  lazy val OutputFields = classOf[EnrichedEvent]
      .getDeclaredFields
      .map(_.getName)

  /**
   * Base64-urlsafe encoded version of this standard
   * Iglu configuration.
   */
  private val IgluConfig = {
    val encoder = new Base64(true) // true means "url safe"
    new String(encoder.encode(
       """|{
            |"schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
            |"data": {
              |"cacheSize": 500,
              |"repositories": [
                |{
                  |"name": "Iglu Central",
                  |"priority": 0,
                  |"vendorPrefixes": [ "com.snowplowanalytics" ],
                  |"connection": {
                    |"http": {
                      |"uri": "http://iglucentral.com"
                    |}
                  |}
                |}
              |]
            |}
          |}""".stripMargin.replaceAll("[\n\r]","").getBytes
      )
    )
  }

  private val oerApiKey = sys.env.get("OER_KEY").getOrElse("-")

  /**
   * User-friendly wrapper to instantiate
   * a BeFieldEqualTo Matcher.
   */
  def beFieldEqualTo(expected: String, withIndex: Int) = new BeFieldEqualTo(expected, withIndex)

  /**
   * A Specs2 matcher to check if a directory
   * on disk is empty or not.
   */
  val beEmptyDir: Matcher[File] =
    ((f: File) => !f.isDirectory || f.list.length > 0, "is populated directory, or not a directory")

  /**
   * A Specs2 matcher to check if a CanonicalOutput
   * field is correctly set.
   *
   * A couple of neat tricks:
   *
   * 1. Skips the comparison if this is the event_id
   *    field, because it has unpredictable values
   * 2. On failure, print out the field's name as
   *    well as the mismatch, to help with debugging
   */
  class BeFieldEqualTo(expected: String, index: Int) extends Matcher[String] {

    private val field = OutputFields(index)
    private val unmatcheable = isUnmatchable(field)

    def apply[S <: String](actual: Expectable[S]) = {
      result((unmatcheable && expected == null) || actual.value == expected,
             "%s: %s".format(field, if (unmatcheable) "is unmatcheable" else "%s equals %s".format(actual.description, expected)),
             "%s: %s does not equal %s".format(field, actual.description, expected),
             actual)
    }

    /**
     * Whether a field in CanonicalOutput is
     * unmatchable - i.e. has unpredictable
     * values.
     *
     * @param field The name of the field
     * @return true if the field is unmatchable,
     *         false otherwise
     */
    private def isUnmatchable(field: String): Boolean =
      UnmatchableFields.contains(field)
  }

  /**
   * A Specs2 matcher to check if a Scalding
   * output sink is empty or not.
   */
  val beEmpty: Matcher[ListBuffer[_]] =
    ((_: ListBuffer[_]).isEmpty, "is not empty")

  /**
   * How Scalding represents input lines
   */
  type ScaldingLines = List[(String, String)]

  /**
   * A case class to make it easy to write out input
   * lines for Scalding jobs without manually appending
   * line numbers.
   *
   * @param l The repeated String parameters
   */
  case class Lines(l: String*) {

    val lines = l.toList
    val numberedLines = number(lines)

    /**
     * Writes the lines to the given file
     *
     * @param file The file to write the
     *        lines to
     */
    def writeTo(file: File) = {
      val writer = new BufferedWriter(new FileWriter(file))
      for (line <- lines) writer.write(line)
      writer.close()
    }

    /**
     * Numbers the lines in the Scalding format.
     * Converts "My line" to ("0" -> "My line")
     *
     * @param lines The List of lines to number
     * @return the List of ("line number" -> "line")
     *         tuples.
     */
    private def number(lines: List[String]): ScaldingLines =
      for ((l, n) <- lines zip (0 until lines.size)) yield (n.toString -> l)
  }

  /**
   * Implicit conversion from a Lines object to
   * a ScaldingLines, aka List[(String, String)],
   * ready for Scalding to use.
   *
   * @param lines The Lines object
   * @return the ScaldingLines ready for Scalding
   */
  implicit def Lines2ScaldingLines(lines : Lines): ScaldingLines = lines.numberedLines 

  /**
   * Creates the the part of the ip_lookups
   * JSON corresponding to a single lookup
   *
   * @param lookup One of the lookup types
   * @return JSON fragment containing the
   *         lookup's database and URI
   */
  def getLookupJson(lookup: String): String = {
     """|"%s": {
          |"database": "%s",
          |"uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
        |}""".format(lookup, lookup match {
      case "geo"          => "GeoIPCity.dat"
      case "isp"          => "GeoIPISP.dat"
      case "organization" => "GeoIPOrg.dat"
      case "domain"       => "GeoIPDomain.dat"
      case "netspeed"     => "GeoIPNetSpeedCell.dat"
      })
  }

  /**
   * Converts the JavaScript script to Base64.
   */
  def getJavascriptScript(): String = {

    val encoder = new Base64(true) // true means "url safe"

    new String(encoder.encode("""|
      |function process(event) {
      |  var appId = event.getApp_id();
      |
      |  if (appId == null) {
      |    return [];
      |  }
      |
      |  var appIdUpper = new String(appId.toUpperCase());
      |  return [ { schema: "iglu:com.acme/app_id/jsonschema/1-0-0",
      |               data:  { appIdUpper: appIdUpper } } ];
      |}
      |""".stripMargin.replaceAll("[\n\r]","").getBytes
    ))
  }

  def getEnrichments(collector: String, anonOctets: String, anonOctetsEnabled: Boolean,
    lookups: List[String], currencyConversionEnabled: Boolean, javascriptScriptEnabled: Boolean): String = {

    val encoder = new Base64(true) // true means "url safe"
    val lookupsJson = lookups.map(getLookupJson(_)).mkString(",\n")
    val jsScript = getJavascriptScript

    new String(encoder.encode(
       s"""|{
            |"schema": "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0",
            |"data": [
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "anon_ip",
                  |"enabled": ${anonOctetsEnabled},
                  |"parameters": {
                    |"anonOctets": ${anonOctets}
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
                    ${lookupsJson}
                  |}
                |}  
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-1",
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
                |"schema": "iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0",
                |"data": {
                  |"enabled": ${currencyConversionEnabled},
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "currency_conversion_config",
                  |"parameters": {
                    |"accountType": "DEVELOPER",
                    |"apiKey": "${oerApiKey}",
                    |"baseCurrency": "EUR",
                    |"rateAt": "EOD_PRIOR"
                  |}
                |}
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/ua_parser_config/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "ua_parser_config",
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
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/javascript_script_config/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "javascript_script_config",
                  |"enabled": ${javascriptScriptEnabled},
                  |"parameters": {
                    |"script": "${jsScript}"
                  |}
                |}
              |}
            |]
          |}""".stripMargin.replaceAll("[\n\r]","").getBytes
      ))
  }

  // Standard JobSpec definition used by all integration tests
  def EtlJobSpec(collector: String, anonOctets: String, anonOctetsEnabled: Boolean, lookups: List[String],
    currencyConversion: Boolean = false, javascriptScript: Boolean = false): JobTest =
    JobTest("com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob").
      arg("input_folder", "inputFolder").
      arg("input_format", collector).
      arg("output_folder", "outputFolder").
      arg("bad_rows_folder", "badFolder").
      arg("etl_tstamp", "1000000000000").      
      arg("iglu_config", IgluConfig).
      arg("enrichments", getEnrichments(collector, anonOctets, anonOctetsEnabled, lookups, currencyConversion, javascriptScript))

  case class Sinks(
    val output:     File,
    val badRows:    File,
    val exceptions: File) {

    def deleteAll() {
      for (f <- List(exceptions, badRows, output)) {
        f.delete()
      }
    }
  }

  /**
   * Run the ShredJob using the Scalding Tool.
   *
   * @param lines The input lines to shred
   * @return a Tuple3 containing open File
   *         objects for the output, bad rows
   *         and exceptions temporary directories.
   */
  def runJobInTool(lines: Lines, collector: String, anonOctets: String, anonOctetsEnabled: Boolean,
    lookups: List[String], currencyConversionEnabled: Boolean = false, javascriptScriptEnabled: Boolean = false): Sinks = {

    def mkTmpDir(tag: String, createParents: Boolean = false, containing: Option[Lines] = None): File = {
      val f = File.createTempFile(s"scala-hadoop-enrich-${tag}-", "")
      if (createParents) f.mkdirs() else f.mkdir()
      containing.map(_.writeTo(f))
      f
    }

    val input      = mkTmpDir("input", createParents = true, containing = lines.some)
    val output     = mkTmpDir("output")
    val badRows    = mkTmpDir("bad-rows")
    val exceptions = mkTmpDir("exceptions")

    val args = Array[String]("com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob", "--local",
      "--input_folder",      input.getAbsolutePath,
      "--input_format",      collector,
      "--output_folder",     output.getAbsolutePath,
      "--bad_rows_folder",   badRows.getAbsolutePath,
      "--exceptions_folder", exceptions.getAbsolutePath,
      "--etl_tstamp",        "1000000000000",
      "--iglu_config",       IgluConfig,
      "--enrichments",       getEnrichments(collector, anonOctets, anonOctetsEnabled, lookups, currencyConversionEnabled, javascriptScriptEnabled))

    // Execute
    Tool.main(args)
    input.delete()

    Sinks(output, badRows, exceptions)
  }
}
