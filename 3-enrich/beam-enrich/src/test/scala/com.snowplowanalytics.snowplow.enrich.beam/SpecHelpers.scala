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
package com.snowplowanalytics.snowplow.enrich.beam

import java.io.File
import java.net.URL
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._
import scala.sys.process._

import cats.Id
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.apache.thrift.TSerializer
import org.scalatest.{Ignore, Tag}

import utils._
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent

object SpecHelpers {

  val resolverConfig = json"""
    {
      "schema": "iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-2",
      "data": {
        "cacheSize": 500,
        "repositories": [
          {
            "name": "Iglu Central",
            "priority": 0,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": { "http": { "uri": "http://iglucentral.com" } }
          },
          {
            "name": "Iglu Central - Mirror 01",
            "priority": 1,
            "vendorPrefixes": [ "com.snowplowanalytics" ],
            "connection": { "http": { "uri": "http://mirror01.iglucentral.com" } }
          }
        ]
      }
    }
  """

  val client = Client
    .parseDefault[Id](resolverConfig)
    .leftMap(_.toString)
    .value
    .fold(
      e => throw new RuntimeException(e),
      r => r
    )

  val enrichmentConfig = json"""
    {
      "schema": "iglu:com.snowplowanalytics.snowplow/anon_ip/jsonschema/1-0-0",
      "data": {
        "name": "anon_ip",
        "vendor": "com.snowplowanalytics.snowplow",
        "enabled": true,
        "parameters": { "anonOctets": 1 }
      }
    }
  """

  val enrichmentsSchemaKey = SchemaKey(
    "com.snowplowanalytics.snowplow",
    "enrichments",
    "jsonschema",
    SchemaVer.Full(1, 0, 0)
  )

  val enrichmentsJson = SelfDescribingData(
    enrichmentsSchemaKey,
    Json.arr(enrichmentConfig)
  )

  val enrichmentConfs = EnrichmentRegistry
    .parse(enrichmentsJson.asJson, client, true)
    .fold(
      e => throw new RuntimeException(e.toList.mkString("\n")),
      r => r
    )

  val enrichmentRegistry = EnrichmentRegistry
    .build(enrichmentConfs)
    .fold(
      e => throw new RuntimeException(e.toList.mkString("\n")),
      r => r
    )

  private val serializer: TSerializer = new TSerializer()
  def buildCollectorPayload(
    body: Option[String] = None,
    contentType: Option[String] = None,
    headers: List[String] = Nil,
    ipAddress: String = "",
    networkUserId: String = java.util.UUID.randomUUID().toString(),
    path: String = "",
    querystring: Option[String] = None,
    refererUri: Option[String] = None,
    timestamp: Long = 0L,
    userAgent: Option[String] = None
  ): Array[Byte] = {
    val cp = new CollectorPayload(
      "iglu:com.snowplowanalytics.snowplow/CollectorPayload/thrift/1-0-0",
      ipAddress,
      timestamp,
      "UTF-8",
      "ssc"
    )
    cp.body = body.orNull
    cp.contentType = contentType.orNull
    cp.hostname = "hostname"
    cp.headers = headers.asJava
    cp.ipAddress = ipAddress
    cp.networkUserId = networkUserId
    cp.path = path
    cp.querystring = querystring.orNull
    cp.refererUri = refererUri.orNull
    cp.userAgent = userAgent.orNull
    serializer.serialize(cp)
  }

  def buildEnrichedEvent(e: String): Map[String, String] = {
    val fields = classOf[EnrichedEvent].getDeclaredFields().map(_.getName())
    val values = e.split("\t")
    fields.zip(values).toMap
  }

  def compareEnrichedEvent(expected: Map[String, String], actual: String): Boolean =
    compareEnrichedEvent(expected, buildEnrichedEvent(actual))

  def compareEnrichedEvent(expected: Map[String, String], actual: Map[String, String]): Boolean =
    expected.forall { case (k, v) => actual.get(k).map(_ == v).getOrElse(false) }

  def write(path: Path, content: String): Unit = {
    Files.write(path, content.getBytes("UTF-8")); ()
  }

  def downloadLocalEnrichmentFile(remoteLocation: String, localLocation: String): Unit = {
    new URL(remoteLocation).#>(new File(localLocation)).!!; ()
  }

  def deleteLocalFile(location: String): Unit = { new File(location).delete; () }

  def copyResource(resource: String, localFile: String): Unit = {
    Files.copy(
      Paths.get(getClass.getResource(resource).toURI()),
      Paths.get(localFile)
    )
    ()
  }
}

object CI
    extends Tag(
      if (sys.env.get("CI").map(_ == "true").getOrElse(false)) "" else classOf[Ignore].getName
    )

object OER extends Tag(if (sys.env.get("OER_KEY").isDefined) "" else classOf[Ignore].getName)
