/*
 * Copyright (c) 2012-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package enrich
package spark

// Java
import java.io.{BufferedWriter, File, FileWriter, IOException}
// Scala
import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random
// Snowplow
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
// Hadoop
import com.hadoop.compression.lzo.GPLNativeCodeLoader
// Commons
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.{FileUtils => FU}
import org.apache.commons.io.filefilter.TrueFileFilter
// circe
import io.circe.Json
import io.circe.parser.parse
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

  val etlVersion =
    s"spark-${generated.BuildInfo.version}-common-${generated.BuildInfo.commonEnrichVersion}"

  val etlTimestamp = "2001-09-09 01:46:40.000"

  private val outputFields = classOf[common.outputs.EnrichedEvent].getDeclaredFields
    .map(_.getName)
  private val unmatchableFields = List("event_id")

  /**
   * Is lzo available?
   */
  def isLzoSupported: Boolean = GPLNativeCodeLoader.isNativeCodeLoaded()

  /**
   * A Specs2 matcher to check if a CanonicalOutput field is correctly set.
   * A couple of neat tricks:
   * 1. Skips the comparison if this is the event_id field, because it has unpredictable values
   * 2. On failure, print out the field's name as well as the mismatch, to help with debugging
   */
  case class BeFieldEqualTo(expected: String, index: Int) extends Matcher[String] {

    private val field        = outputFields(index)
    private val unmatcheable = isUnmatchable(field)

    def apply[S <: String](actual: Expectable[S]) =
      result(
        (unmatcheable && expected == null) || actual.value == expected,
        if (unmatcheable) s"$field (index: $index): is unmatcheable"
        else s"$field (index: $index): ${actual.description} equals $expected",
        s"$field (index: $index): ${actual.description} does not equal $expected",
        actual
      )

    /**
     * Whether a field in CanonicalOutput is unmatchable - i.e. has unpredictable values.
     * @param field The name of the field
     * @return true if the field is unmatchable, false otherwise
     */
    private def isUnmatchable(field: String): Boolean = unmatchableFields.contains(field)
  }

  /** A Specs2 matcher to check if a directory on disk is empty or not. */
  val beEmptyDir: Matcher[File] =
    (
      (f: File) =>
        !f.isDirectory ||
          f.list().length == 0 ||
          f.listFiles()
            .filter(f => f.getName != "_SUCCESS" && !f.getName.endsWith(".crc"))
            .map(_.length)
            .sum == 0,
      "is populated dir")

  /**
   * Needed for the different specs using fors since it results in AsResult[Unit] which isn't one
   * of the supported AsResult typeclasses.
   */
  implicit def unitAsResult: AsResult[Unit] = new AsResult[Unit] {
    def asResult(r: => Unit) = ResultExecution.execute(r)(_ => org.specs2.execute.Success())
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
        val list = Source.fromFile(new File(f)).getLines.toList
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
    def listFilesSafely(file: File): Seq[File] =
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) throw new IOException(s"Failed to list files for dir: $file")
        files
      } else {
        Seq.empty[File]
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
    new File(
      System.getProperty("java.io.tmpdir"),
      s"snowplow-enrich-job-${tag}-${Random.nextInt(Int.MaxValue)}")

  /** Remove the timestamp from bad rows so that what remains is deterministic */
  def removeTstamp(json: String): String =
    parse(json) match {
      case Right(j) =>
        j.hcursor
          .downField("data")
          .downField("failure")
          .downField("timestamp")
          .set(Json.fromString("2019-11-22T09:37:21.643Z"))
          .top
          .getOrElse(j)
          .noSpaces
      case Left(_) => json
    }

  /** Remove the timestamp from bad rows so that what remains is deterministic */
  def removeId(json: String): String =
    parse(json) match {
      case Right(j) =>
        j.hcursor
          .downField("data")
          .downField("payload")
          .downField("enriched")
          .downField("event_id")
          .set(Json.fromString("ff30c659-682a-4bf6-a378-4081d3c34c76"))
          .top
          .getOrElse(Json.Null)
          .noSpaces
      case Left(_) => json
    }

  /**
   * Base64-encoded urlsafe resolver config used by default if `HADOOP_ENRICH_RESOLVER_CONFIG` env
   * var is not set.
   */
  private val igluCentralConfig = {
    val encoder = new Base64(true)
    new String(
      encoder.encode( //TODO: remove YAUAA Iglu when https://github.com/snowplow/iglu-central/pull/923 is merged
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
      |}""".stripMargin.replaceAll("[\n\r]", "").getBytes()))
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

  /** Builds the JSON holding the configuration for all the enrichments. */
  def getEnrichments(
    anonOctets: String,
    anonOctetsEnabled: Boolean,
    lookups: List[String],
    currencyConversionEnabled: Boolean,
    javascriptScriptEnabled: Boolean,
    apiRequest: Boolean,
    sqlQuery: Boolean,
    iabEnrichmentEnabled: Boolean,
    yauaaEnrichmentEnabled: Boolean = false
  ): String = {

    /**
     * Creates the the part of the ip_lookups JSON corresponding to a single lookup
     * @param lookup One of the lookup types
     * @return JSON fragment containing the lookup's database and URI
     */
    def getLookupJson(lookup: String): String =
      """|"%s": {
            |"database": "%s",
            |"uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/maxmind"
          |}""".format(
        lookup,
        lookup match {
          case "geo"            => "GeoIP2-City.mmdb"
          case "isp"            => "GeoIP2-ISP.mmdb"
          case "domain"         => "GeoIP2-Domain.mmdb"
          case "connectionType" => "GeoIP2-Connection-Type.mmdb"
        }
      )

    def getIabJson(database: String): String = {
      """|"%s": {
         |"database": "%s",
         |"uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/iab"
         |}""".format(
        database,
        database match {
          case "ipFile"               => "ip_exclude_current_cidr.txt"
          case "excludeUseragentFile" => "exclude_current.txt"
          case "includeUseragentFile" => "include_current.txt"
        }
      )
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
        |""".stripMargin.replaceAll("[\n\r]", "").getBytes))
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
    new String(encoder.encode(s"""|{
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
                |"schema": "iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0",
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
                |"schema": "iglu:com.snowplowanalytics.snowplow.enrichments/iab_spiders_and_robots_enrichment/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "iab_spiders_and_robots_enrichment",
                  |"enabled": ${iabEnrichmentEnabled},
                  |"parameters": {
                    ${List("ipFile", "excludeUseragentFile", "includeUseragentFile")
                                   .map(getIabJson(_))
                                   .mkString(",\n")}
                  |}
                |}
              |},
              |{
                |"schema": "iglu:com.snowplowanalytics.snowplow.enrichments/yauaa_enrichment_config/jsonschema/1-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "yauaa_enrichment_config",
                  |"enabled": $yauaaEnrichmentEnabled
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
                |"schema": "iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0",
                |"data": {
                  |"vendor": "com.snowplowanalytics.snowplow",
                  |"name": "referer_parser",
                  |"enabled": true,
                  |"parameters": {
                    |"internalDomains": ["www.subdomain1.snowplowanalytics.com"],
                    |"database": "referer-tests.json",
                    |"uri": "http://snowplow-hosted-assets.s3.amazonaws.com/third-party/referer-parser"
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
                        |"salt": "pink123"
                      |}
                    |}
                  |}
                |}
              |}
            |]
          |}""".stripMargin.replaceAll("[\n\r]", "").getBytes))
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
    sqlQueryEnabled: Boolean = false,
    iabEnrichmentEnabled: Boolean = false,
    yauaaEnrichmentEnabled: Boolean = false
  ): Unit = {
    val input = mkTmpFile("input", lines)
    runEnrichJob(
      input.toString(),
      collector,
      anonOctets,
      anonOctetsEnabled,
      lookups,
      currencyConversionEnabled,
      javascriptScriptEnabled,
      apiRequestEnabled,
      sqlQueryEnabled,
      iabEnrichmentEnabled,
      yauaaEnrichmentEnabled
    )
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
    sqlQueryEnabled: Boolean,
    iabEnrichmentEnabled: Boolean,
    yauaaEnrichmentEnabled: Boolean
  ): Unit = {
    val config = Array(
      "--input-folder",
      inputFile,
      "--input-format",
      collector,
      "--output-folder",
      dirs.output.toString(),
      "--bad-folder",
      dirs.badRows.toString(),
      "--enrichments",
      getEnrichments(
        anonOctets,
        anonOctetsEnabled,
        lookups,
        currencyConversionEnabled,
        javascriptScriptEnabled,
        apiRequestEnabled,
        sqlQueryEnabled,
        iabEnrichmentEnabled,
        yauaaEnrichmentEnabled
      ),
      "--iglu-config",
      igluConfig,
      "--etl-timestamp",
      1000000000000L.toString,
      "--local"
    )

    EnrichJob(spark, config).run()
  }

  /**
   * Write a LZO file with a random name.
   * @param tag an identifier who will become part of the file name
   * @param payload the content of the file
   * @return the created file which was written to
   */
  def writeLzo(tag: String, payload: CollectorPayload): File = {
    import com.twitter.elephantbird.mapreduce.io.ThriftWritable
    import com.twitter.elephantbird.mapreduce.output.LzoThriftBlockOutputFormat
    import org.apache.hadoop.io.LongWritable
    val f = new File(
      System.getProperty("java.io.tmpdir"),
      s"snowplow-enrich-job-${tag}-${scala.util.Random.nextInt(Int.MaxValue)}")
    val rdd = spark.sparkContext
      .parallelize(Seq(payload))
      .map { e =>
        val writable = ThriftWritable.newInstance(classOf[CollectorPayload])
        writable.set(e)
        (new LongWritable(1L), writable)
      }
    LzoThriftBlockOutputFormat.setClassConf(classOf[CollectorPayload], hadoopConfig)
    rdd.saveAsNewAPIHadoopFile(
      f.toString(),
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[ThriftWritable[CollectorPayload]],
      classOf[LzoThriftBlockOutputFormat[ThriftWritable[CollectorPayload]]],
      hadoopConfig
    )
    f
  }

  override def afterAll(): Unit = {
    super.afterAll()
    dirs.deleteAll()
  }
}
