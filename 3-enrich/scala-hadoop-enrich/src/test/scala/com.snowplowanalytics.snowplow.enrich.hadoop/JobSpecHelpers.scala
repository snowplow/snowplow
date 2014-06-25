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

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Scala
import scala.collection.mutable.ListBuffer

// Specs2
import org.specs2.matcher.{Matcher, Expectable}
import org.specs2.matcher.Matchers._

// Scalding
import com.twitter.scalding._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

// Snowplow Common Enrich
import common.outputs.CanonicalOutput

/**
 * Holds helpers for running integration
 * tests on SnowPlow EtlJobs.
 */
object JobSpecHelpers {

  /**
   * The current version of our Hadoop ETL
   */
  val EtlVersion = "hadoop-0.5.0-common-0.5.0-SNAPSHOT"

  val EtlTimestamp = "2001-09-09 01:46:40.000"

  /**
   * Fields in our CanonicalOutput which are unmatchable
   */
  private val UnmatchableFields = List("event_id")

  /**
   * The names of the fields written out
   */
  lazy val OutputFields = classOf[CanonicalOutput]
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

  /**
   * User-friendly wrapper to instantiate
   * a BeFieldEqualTo Matcher.
   */
  def beFieldEqualTo(expected: String, withIndex: Int) = new BeFieldEqualTo(expected, withIndex)

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
      result(unmatcheable || actual.value == expected,
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

  // Standard JobSpec definition used by all integration tests
  val EtlJobSpec: (String, String, Boolean) => JobTest = (collector, anonOctets, anonOctetsEnabled) => {

    val encoder = new Base64(true) // true means "url safe"
    val enrichments = new String(encoder.encode(
       """|{
            |"schema": "iglu:com.snowplowanalytics.snowplow/enrichments/jsonschema/1-0-0",
            |"data": [
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics",
                  |"name": "anon_ip",
                  |"enabled": %s,
                  |"parameters": {
                    |"anonOctets": %s
                  |}
                |}
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/ip_to_geo/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics",
                  |"name": "ip_to_geo",
                  |"enabled": true,
                  |"parameters": {
                    |"maxmindDatabase": "GeoLiteCity.dat",
                    |"maxmindUri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind/"
                  |}
                |}  
              |}
            |]
          |}""".format(anonOctetsEnabled, anonOctets).stripMargin.replaceAll("[\n\r]","").getBytes
      )
    )

    JobTest("com.snowplowanalytics.snowplow.enrich.hadoop.EtlJob").
      arg("input_folder", "inputFolder").
      arg("input_format", collector).
      arg("output_folder", "outputFolder").
      arg("bad_rows_folder", "badFolder").
      arg("etl_tstamp", "1000000000000").      
      //arg("exceptions_folder", "exceptionsFolder").
      arg("iglu_config", IgluConfig).
      arg("enrichments", enrichments)
    }
}
