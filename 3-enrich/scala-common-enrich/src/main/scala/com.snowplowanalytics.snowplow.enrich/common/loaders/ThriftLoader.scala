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
package com.snowplowanalytics
package snowplow.enrich.common
package loaders

// Apache Commons
import org.apache.commons.lang3.StringUtils

// Apache URLEncodedUtils
import org.apache.http.NameValuePair

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone}

// Thrift
import org.apache.thrift.TDeserializer

// Java conversions
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Iglu
import iglu.client.{SchemaKey, SchemaCriterion}

// Snowplow
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.{
  CollectorPayload => CollectorPayload1
}
import com.snowplowanalytics.snowplow.SchemaSniffer.thrift.model1.SchemaSniffer
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

/**
 * Loader for Thrift SnowplowRawEvent objects.
 */
object ThriftLoader extends Loader[Array[Byte]] {
  
  private val thriftDeserializer = new TDeserializer

  private val ExpectedSchema = SchemaCriterion("com.snowplowanalytics.snowplow", "CollectorPayload", "thrift", 1, 0)

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * Checks the version of the raw event and calls the appropriate method.
   *
   * @param line A serialized Thrift object Byte array mapped to a String.
   *   The method calling this should encode the serialized object
   *   with `snowplowRawEventBytes.map(_.toChar)`.
   *   Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed
   *         CanonicalInput object, wrapped in a Scalaz ValidatioNel.
   */
  def toCollectorPayload(line: Array[Byte]): ValidatedMaybeCollectorPayload = {

    try {

      var schema = new SchemaSniffer

      this.synchronized {
        thriftDeserializer.deserialize(
          schema,
          line
        )
      }

      if (schema.isSetSchema) {
        val actualSchema = SchemaKey.parse(schema.getSchema).leftMap(_.toString).toValidationNel

        for {
          as <- actualSchema
          res <- if (ExpectedSchema.matches(as)) {
              convertSchema1(line)
            } else {
              s"Verifying record as $ExpectedSchema failed: found $as".failNel
            }
        } yield res

      } else {
        convertOldSchema(line)
      }
    } catch {
      // TODO: Check for deserialization errors.
      case e: Throwable =>
        s"Error deserializing raw event: ${e.getMessage}".failNel[Option[CollectorPayload]]
    }
  }

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * Assumes that the byte array is a serialized CollectorPayload, version 1.
   *
   * @param line A serialized Thrift object Byte array mapped to a String.
   *   The method calling this should encode the serialized object
   *   with `snowplowRawEventBytes.map(_.toChar)`.
   *   Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed
   *         CanonicalInput object, wrapped in a Scalaz ValidatioNel.
   */
  private def convertSchema1(line: Array[Byte]): ValidatedMaybeCollectorPayload = {

    var collectorPayload = new CollectorPayload1
    this.synchronized {
      thriftDeserializer.deserialize(
        collectorPayload,
        line
      )
    }

    val querystring = parseQuerystring(
      Option(collectorPayload.querystring),
      collectorPayload.encoding
    )

    val hostname = Option(collectorPayload.hostname)
    val userAgent = Option(collectorPayload.userAgent)
    val refererUri = Option(collectorPayload.refererUri)
    val networkUserId = Option(collectorPayload.networkUserId)

    val headers = Option(collectorPayload.headers)
      .map(_.toList).getOrElse(Nil)

    val ip = IpAddressExtractor.extractIpAddress(headers, collectorPayload.ipAddress).some // Required

    val api = Option(collectorPayload.path) match {
      case None => "Request does not contain a path".fail
      case Some(p) => CollectorApi.parse(p)
    }

    (querystring.toValidationNel |@|
      api.toValidationNel) { (q: List[NameValuePair], a: CollectorApi) => CollectorPayload(
        q,
        collectorPayload.collector,
        collectorPayload.encoding,
        hostname,
        Some(new DateTime(collectorPayload.timestamp, DateTimeZone.UTC)),
        ip,
        userAgent,
        refererUri,
        headers,
        networkUserId,
        a,
        Option(collectorPayload.contentType),
        Option(collectorPayload.body)
        ).some
    }
  }

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * Assumes that the byte array is an old serialized SnowplowRawEvent
   * which is not self-describing.
   *
   * @param line A serialized Thrift object Byte array mapped to a String.
   *   The method calling this should encode the serialized object
   *   with `snowplowRawEventBytes.map(_.toChar)`.
   *   Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed
   *         CanonicalInput object, wrapped in a Scalaz ValidatioNel.
   */
  private def convertOldSchema(line: Array[Byte]): ValidatedMaybeCollectorPayload = {

    var snowplowRawEvent = new SnowplowRawEvent
    this.synchronized {
      thriftDeserializer.deserialize(
        snowplowRawEvent,
        line
      )
    }

    val querystring = parseQuerystring(
      Option(snowplowRawEvent.payload.data),
      snowplowRawEvent.encoding
    )

    val hostname = Option(snowplowRawEvent.hostname)
    val userAgent = Option(snowplowRawEvent.userAgent)
    val refererUri = Option(snowplowRawEvent.refererUri)
    val networkUserId = Option(snowplowRawEvent.networkUserId)

    val headers = Option(snowplowRawEvent.headers)
      .map(_.toList).getOrElse(Nil)

    val ip = IpAddressExtractor.extractIpAddress(headers, snowplowRawEvent.ipAddress).some // Required

    (querystring.toValidationNel) map { (q: List[NameValuePair]) =>
      Some(
        CollectorPayload(
          q,
          snowplowRawEvent.collector,
          snowplowRawEvent.encoding,
          hostname,
          Some(new DateTime(snowplowRawEvent.timestamp, DateTimeZone.UTC)),
          ip,
          userAgent,
          refererUri,
          headers,
          networkUserId,
          CollectorApi.SnowplowTp1, // No way of storing API vendor/version in Thrift yet, assume Snowplow TP1
          None, // No way of storing content type in Thrift yet
          None  // No way of storing request body in Thrift yet
        )
      )
    }
  }
}
