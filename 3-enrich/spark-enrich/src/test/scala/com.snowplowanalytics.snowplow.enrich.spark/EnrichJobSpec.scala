/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowplow.enrich
package spark

// Java
import java.io.{BufferedWriter, File, FileWriter, IOException}

// Scala
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

// Commons
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.{FileUtils => FU}
import org.apache.commons.io.filefilter.TrueFileFilter

// Json4s
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse}

// Scalaz
import scalaz._
import Scalaz._

// Specs2
import org.specs2.execute.{AsResult, ResultExecution}
import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.matcher.Matchers._

object EnrichJobSpec {
  /** Case class representing the input lines written in a file. */
  case class Lines(l: String*) {
    val lines = l.toList

    /** Write the lines to a file. */
    def writeTo(file: File): Unit = {
      val writer = new BufferedWriter(new FileWriter(file))
      for (line <- lines) {
        writer.write(line)
        writer.newLine()
      }
      writer.close()
    }

    def apply(i: Int): String = lines(i)
  }

  /** Case class representing the directories where the output of the job has been written. */
  case class OutputDirs(output: File, badRows: File) {
    /** Delete recursively the output and bad rows directories. */
    def deleteAll(): Unit = List(badRows, output).foreach(deleteRecursively)
  }

  val etlVersion = "spark-1.9.0-rc1-common-0.25.0-M4"

  val etlTimestamp = "2001-09-09 01:46:40.000"

  private val outputFields = classOf[common.outputs.EnrichedEvent]
    .getDeclaredFields
    .map(_.getName)
  private val unmatchableFields = List("event_id")

  /**
   * A Specs2 matcher to check if a CanonicalOutput field is correctly set.
   * A couple of neat tricks:
   * 1. Skips the comparison if this is the event_id field, because it has unpredictable values
   * 2. On failure, print out the field's name as well as the mismatch, to help with debugging
   */
  case class BeFieldEqualTo(expected: String, index: Int) extends Matcher[String] {

    private val field = outputFields(index)
    private val unmatcheable = isUnmatchable(field)

    def apply[S <: String](actual: Expectable[S]) = {
      result((unmatcheable && expected == null) || actual.value == expected,
        "%s: %s".format(field, if (unmatcheable) "is unmatcheable" else "%s equals %s".format(actual.description, expected)),
        "%s: %s does not equal %s".format(field, actual.description, expected),
        actual)
    }

    /**
     * Whether a field in CanonicalOutput is unmatchable - i.e. has unpredictable values.
     * @param field The name of the field
     * @return true if the field is unmatchable, false otherwise
     */
    private def isUnmatchable(field: String): Boolean = unmatchableFields.contains(field)
  }

  /** A Specs2 matcher to check if a directory on disk is empty or not. */
  val beEmptyDir: Matcher[File] =
    ((f: File) =>
      !f.isDirectory ||
        f.list().length == 0 ||
        f.listFiles().filter(f => f.getName != "_SUCCESS" && !f.getName.endsWith(".crc")).map(_.length).sum == 0,
      "is populated dir")

  /**
   * Needed for the different specs using fors since it results in AsResult[Unit] which isn't one
   * of the supported AsResult typeclasses.
   */
  implicit def unitAsResult: AsResult[Unit] = new AsResult[Unit] {
    def asResult(r: =>Unit) = ResultExecution.execute(r)(_ => org.specs2.execute.Success())
  }

  /**
   * Read a part file at the given path into a List of Strings
   * @param root A root filepath
   * @return the file contents
   */
  def readPartFile(root: File): Option[List[String]] = {
    val files = listFilesWithExclusions(root, List.empty)
      .filter(s => s.contains("part-") && !s.contains("crc"))
    files.headOption match {
      case Some(f) =>
        val list = Source.fromFile(new File(f))
          .getLines
          .toList
        Some(list)
      case None => None
    }
  }

  /**
   * Recursively list files in a given path, excluding the supplied paths.
   * @param root A root filepath
   * @param exclusions A list of paths to exclude from the listing
   * @return the list of files contained in the root, minus the exclusions
   */
  def listFilesWithExclusions(root: File, exclusions: List[String]): List[String] =
    FU.listFiles(root, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)
      .asScala
      .toList
      .map(_.getCanonicalPath)
      .filter(p => !exclusions.contains(p) && !p.contains("crc") && !p.contains("SUCCESS"))

  /**
   * Delete a file or directory and its contents recursively.
   * Throws an exception if deletion is unsuccessful.
   */
  def deleteRecursively(file: File): Unit = {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) throw new IOException(s"Failed to list files for dir: $file")
        files
      } else {
        Seq.empty[File]
      }
    }

    try {
      if (file.isDirectory) {
        var savedIOException: IOException = null
        for (child <- listFilesSafely(file)) {
          try {
            deleteRecursively(child)
          } catch {
            // In case of multiple exceptions, only last one will be thrown
            case ioe: IOException => savedIOException = ioe
          }
        }
        if (savedIOException != null) throw savedIOException
      }
    } finally {
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) throw new IOException(s"Failed to delete: ${file.getAbsolutePath}")
      }
    }
  }

  /**
   * Make a temporary file filling it with contents.
   * @param tag an identifier who will become part of the file name
   * @param containing the content
   * @return the created file
   */
  def mkTmpFile(tag: String, containing: Lines): File = {
    val f = File.createTempFile(s"snowplow-enrich-job-${tag}-", "")
    f.mkdirs()
    containing.writeTo(f)
    f
  }

  /**
   * Reference a file without creating it with the specified name with a random number at the end.
   * @param tag an identifier who will become part of the file name
   * @return the created file
   */
  def randomFile(tag: String): File =
    new File(System.getProperty("java.io.tmpdir"),
      s"snowplow-enrich-job-${tag}-${Random.nextInt(Int.MaxValue)}")

  /** Remove the timestamp from bad rows so that what remains is deterministic */
  def removeTstamp(badRow: String): String = {
    val badRowJson = parse(badRow)
    val badRowWithoutTimestamp =
      ("line", (badRowJson \ "line")) ~ (("errors", (badRowJson \ "errors")))
      compact(badRowWithoutTimestamp)
  }

  /**
   * Base64-encoded urlsafe resolver config used by default if `HADOOP_ENRICH_RESOLVER_CONFIG` env
   * var is not set.
   */
  private val igluCentralConfig = {
    val encoder = new Base64(true)
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
      |}""".stripMargin.replaceAll("[\n\r]","").getBytes()
    ))
  }

  /**
   * Try to take resolver configuration from environment variable `HADOOP_ENRICH_RESOLVER_CONFIG`
   * (must be base64-encoded). If not available - use default config with only Iglu Central
   */
  private val igluConfig = {
    val resolverEnvVar = for {
      config <- sys.env.get("HADOOP_ENRICH_RESOLVER_CONFIG")
      if config.nonEmpty
    } yield config
    resolverEnvVar.getOrElse(igluCentralConfig)
  }

  def getEnrichments(
    anonOctets: String,
    anonOctetsEnabled: Boolean,
    lookups: List[String],
    currencyConversionEnabled: Boolean,
    javascriptScriptEnabled: Boolean,
    apiRequest: Boolean,
    sqlQuery: Boolean
  ): String = {

    /**
     * Creates the the part of the ip_lookups JSON corresponding to a single lookup
     * @param lookup One of the lookup types
     * @return JSON fragment containing the lookup's database and URI
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

    /** Converts the JavaScript script to Base64. */
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

    val sqlQueryParameters =
      """
      |{
        |"inputs": [
          |{
            |"placeholder": 1,
            |"pojo": {
              |"field": "geo_city"
            |}
          |},
          |{
            |"placeholder": 2,
            |"json": {
              |"field": "contexts",
              |"schemaCriterion": "iglu:com.snowplowanalytics.snowplow/geolocation_context/jsonschema/*-*-*",
              |"jsonPath": "$.speed"
            |}
          |}
        |],
        |"database": {
          |"postgresql": {
            |"host": "localhost",
            |"port": 5432,
            |"sslMode": false,
            |"username": "enricher",
            |"password": "supersecret1",
            |"database": "sql_enrichment_test"
          |}
        |},
        |"query": {
          |"sql": "SELECT city, country, pk FROM enrichment_test WHERE city = ? and speed = ?"
        |},
        |"output": {
          |"expectedRows": "AT_LEAST_ONE",
          |"json": {
            |"schema": "iglu:com.acme/user/jsonschema/1-0-0",
            |"describes": "ALL_ROWS",
            |"propertyNames": "CAMEL_CASE"
          |}
        |},
        |"cache": {
          |"size": 1000,
          |"ttl": 60
        |}
      |}""".stripMargin

    // Added separately because dollar sign in JSONPath breaks interpolation
    val apiRequestParameters =
      """
      |{
        |"inputs": [
          |{
            |"key": "uhost",
            |"pojo": {
              |"field": "page_urlhost"
            |}
          |},
          |{
            |"key": "os",
            |"json": {
              |"field": "derived_contexts",
              |"schemaCriterion": "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-*",
              |"jsonPath": "$.osFamily"
            |}
          |},
          |{
            |"key": "device",
            |"json": {
              |"field": "contexts",
              |"schemaCriterion": "iglu:com.snowplowanalytics.snowplow/mobile_context/jsonschema/1-*-*",
              |"jsonPath": "$.deviceModel"
            |}
          |},
          |{
            |"key": "service",
            |"json": {
              |"field": "unstruct_event",
              |"schemaCriterion": "iglu:com.snowplowanalytics.snowplow-website/signup_form_submitted/jsonschema/1-0-*",
              |"jsonPath": "$.serviceType"
            |}
          |}
        |],
        |"api": {
          |"http": {
            |"method": "GET",
            |"uri": "http://localhost:8000/guest/users/{{device}}/{{os}}/{{uhost}}/{{service}}?format=json",
            |"timeout": 2000,
            |"authentication": { }
          |}
        |},
        |"outputs": [
          |{
            |"schema": "iglu:com.acme/user/jsonschema/1-0-0" ,
            |"json": {
              |"jsonPath": "$"
            |}
          |}
        |],
        |"cache": {
          |"size": 2,
          |"ttl": 60
        |}
      |}""".stripMargin

    val encoder = new Base64(true) // true means "url safe"
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
                    ${lookups.map(getLookupJson(_)).mkString(",\n")}
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
                    |"apiKey": "${sys.env.get("OER_KEY").getOrElse("-")}",
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
                    |"script": "${getJavascriptScript()}"
                  |}
                |}
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow/event_fingerprint_config/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "event_fingerprint_config",
                  |"enabled": true,
                  |"parameters": {
                    |"excludeParameters": ["eid", "stm"],
                    |"hashAlgorithm": "MD5"
                  |}
                |}
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow.enrichments/api_request_enrichment_config/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow.enrichments",
                  |"name": "api_request_enrichment_config",
                  |"enabled": ${apiRequest},
                  |"parameters": ${apiRequestParameters}
                |}
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow.enrichments/sql_query_enrichment_config/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow.enrichments",
                  |"name": "sql_query_enrichment_config",
                  |"enabled": ${sqlQuery},
                  |"parameters": ${sqlQueryParameters}
                |}
              |}
            |]
          |}""".stripMargin.replaceAll("[\n\r]","").getBytes
      ))
  }
}

/** Trait to mix in in every spec for the enrich job. */
trait EnrichJobSpec extends SparkSpec {
  import EnrichJobSpec._
  val dirs = OutputDirs(randomFile("output"), randomFile("bad-rows"))

  /**
   * Run the enrich job with the specified lines as input.
   * @param lines input lines
   * @param collector collector used
   */
  def runEnrichJob(
    lines: Lines,
    collector: String,
    anonOctets: String,
    anonOctetsEnabled: Boolean,
    lookups: List[String],
    currencyConversionEnabled: Boolean = false,
    javascriptScriptEnabled: Boolean = false,
    apiRequestEnabled: Boolean = false,
    sqlQueryEnabled: Boolean = false
  ): Unit = {
    val input = mkTmpFile("input", lines)
    runEnrichJob(input.toString(), collector, anonOctets, anonOctetsEnabled, lookups,
      currencyConversionEnabled, javascriptScriptEnabled, apiRequestEnabled, sqlQueryEnabled)
    deleteRecursively(input)
  }

  /**
   * Run the enrich job with the specified lines as input.
   * @param lines input lines
   * @param collector collector used
   */
  def runEnrichJob(
    inputFile: String,
    collector: String,
    anonOctets: String,
    anonOctetsEnabled: Boolean,
    lookups: List[String],
    currencyConversionEnabled: Boolean,
    javascriptScriptEnabled: Boolean,
    apiRequestEnabled: Boolean,
    sqlQueryEnabled: Boolean
  ): Unit = {
    val config = Array(
      "--input-folder", inputFile,
      "--input-format", collector,
      "--output-folder", dirs.output.toString(),
      "--bad-folder", dirs.badRows.toString(),
      "--enrichments", getEnrichments(anonOctets, anonOctetsEnabled, lookups,
        currencyConversionEnabled, javascriptScriptEnabled, apiRequestEnabled, sqlQueryEnabled),
      "--iglu-config", igluConfig,
      "--etl-timestamp", 1000000000000L.toString,
      "--local"
    )

    val job = EnrichJob(spark, config)
    job.run()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dirs.deleteAll()
  }
}
