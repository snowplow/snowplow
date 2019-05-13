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
package com.snowplowanalytics.snowplow.enrich.common
package loaders

import java.nio.charset.Charset
import java.time.Instant
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import cats.data.{NonEmptyList, ValidatedNel}
import cats.implicits._

import org.joda.time.{DateTime, DateTimeZone}

import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.{
  CollectorPayload => CollectorPayload1
}
import com.snowplowanalytics.snowplow.SchemaSniffer.thrift.model1.SchemaSniffer
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.badrows.FailureDetails.CPFormatViolationMessage

import org.apache.commons.codec.binary.Base64
import org.apache.thrift.TDeserializer

import com.snowplowanalytics.iglu.core.{SchemaCriterion, SchemaKey, ParseError => IgluParseError}

/** Loader for Thrift SnowplowRawEvent objects. */
object ThriftLoader extends Loader[Array[Byte]] {
  private val thriftDeserializer = new TDeserializer

  private val ExpectedSchema =
    SchemaCriterion("com.snowplowanalytics.snowplow", "CollectorPayload", "thrift", 1, 0)

  /** Parse Error -> Collector Payload violation */
  private def collectorPayloadViolation(e: IgluParseError) = {
    val str = s"could not parse schema: ${e.code}"
    val details = FailureDetails.CPFormatViolationMessage.Fallback(str)
    NonEmptyList.of(details)
  }

  private def collectorPayloadViolation(key: SchemaKey) = {
    val str = s"verifying record as ${ExpectedSchema.asString} failed: found ${key.toSchemaUri}"
    val details = FailureDetails.CPFormatViolationMessage.Fallback(str)
    NonEmptyList.of(details)
  }

  /**
   * Converts the source string into a [[CollectorPayload]] (always `Some`)
   * Checks the version of the raw event and calls the appropriate method.
   * @param line A serialized Thrift object Byte array mapped to a String. The method calling this
   * should encode the serialized object with `snowplowRawEventBytes.map(_.toChar)`.
   * Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
   * a ValidatedNel.
   */
  override def toCollectorPayload(
    line: Array[Byte],
    processor: Processor
  ): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] = {

    def createViolation(message: FailureDetails.CPFormatViolationMessage) =
      BadRow.CPFormatViolation(
        processor,
        Failure.CPFormatViolation(Instant.now(), "thrift", message),
        Payload.RawPayload(new String(Base64.encodeBase64(line), "UTF-8"))
      )

    val collectorPayload =
      try {
        val schema = new SchemaSniffer()
        this.synchronized { thriftDeserializer.deserialize(schema, line) }
        if (schema.isSetSchema) {
          val payload = for {
            schemaKey <- SchemaKey.fromUri(schema.getSchema).leftMap(collectorPayloadViolation)
            collectorPayload <- if (ExpectedSchema.matches(schemaKey)) convertSchema1(line).toEither
            else collectorPayloadViolation(schemaKey).asLeft
          } yield collectorPayload
          payload.toValidated
        } else convertOldSchema(line)
      } catch {
        case NonFatal(e) =>
          FailureDetails.CPFormatViolationMessage
            .Fallback(s"error deserializing raw event: ${e.getMessage}")
            .invalidNel
      }

    collectorPayload.leftMap { messages =>
      messages.map(createViolation)
    }
  }

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * Assumes that the byte array is a serialized CollectorPayload, version 1.
   * @param line A serialized Thrift object Byte array mapped to a String. The method calling this
   * should encode the serialized object with`snowplowRawEventBytes.map(_.toChar)`.
   * Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
   * a ValidatedNel.
   */
  private def convertSchema1(
    line: Array[Byte]
  ): ValidatedNel[FailureDetails.CPFormatViolationMessage, Option[CollectorPayload]] = {
    val collectorPayload = new CollectorPayload1
    this.synchronized {
      thriftDeserializer.deserialize(
        collectorPayload,
        line
      )
    }

    val querystring = parseQuerystring(
      Option(collectorPayload.querystring),
      Charset.forName(collectorPayload.encoding)
    )

    val hostname = Option(collectorPayload.hostname)
    val userAgent = Option(collectorPayload.userAgent)
    val refererUri = Option(collectorPayload.refererUri)
    val networkUserId =
      Option(collectorPayload.networkUserId).traverse(parseNetworkUserId).toValidatedNel

    val headers = Option(collectorPayload.headers).map(_.asScala.toList).getOrElse(Nil)

    val ip = IpAddressExtractor.extractIpAddress(headers, collectorPayload.ipAddress).some // Required

    val api = Option(collectorPayload.path) match {
      case None =>
        FailureDetails.CPFormatViolationMessage
          .InputData("path", None, "request does not contain a path")
          .invalidNel
      case Some(p) => CollectorPayload.parseApi(p).toValidatedNel
    }

    (querystring.toValidatedNel, api, networkUserId).mapN { (q, a, nuid) =>
      val source =
        CollectorPayload.Source(collectorPayload.collector, collectorPayload.encoding, hostname)
      val context = CollectorPayload.Context(
        Some(new DateTime(collectorPayload.timestamp, DateTimeZone.UTC)),
        ip,
        userAgent,
        refererUri,
        headers,
        nuid
      )
      CollectorPayload(
        a,
        q,
        Option(collectorPayload.contentType),
        Option(collectorPayload.body),
        source,
        context
      ).some
    }
  }

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload. Assumes that the byte array
   * is an old serialized SnowplowRawEvent which is not self-describing.
   * @param line A serialized Thrift object Byte array mapped to a String. The method calling this
   * should encode the serialized object with `snowplowRawEventBytes.map(_.toChar)`.
   * Reference: http://stackoverflow.com/questions/5250324/
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped in
   * a ValidatedNel.
   */
  private def convertOldSchema(
    line: Array[Byte]
  ): ValidatedNel[FailureDetails.CPFormatViolationMessage, Option[CollectorPayload]] = {
    val snowplowRawEvent = new SnowplowRawEvent()
    this.synchronized {
      thriftDeserializer.deserialize(
        snowplowRawEvent,
        line
      )
    }

    val querystring = parseQuerystring(
      Option(snowplowRawEvent.payload.data),
      Charset.forName(snowplowRawEvent.encoding)
    )

    val hostname = Option(snowplowRawEvent.hostname)
    val userAgent = Option(snowplowRawEvent.userAgent)
    val refererUri = Option(snowplowRawEvent.refererUri)
    val networkUserId =
      Option(snowplowRawEvent.networkUserId).traverse(parseNetworkUserId).toValidatedNel

    val headers = Option(snowplowRawEvent.headers).map(_.asScala.toList).getOrElse(Nil)

    val ip = IpAddressExtractor.extractIpAddress(headers, snowplowRawEvent.ipAddress).some // Required

    (querystring.toValidatedNel, networkUserId).mapN { (q, nuid) =>
      val timestamp = Some(new DateTime(snowplowRawEvent.timestamp, DateTimeZone.UTC))
      val context = CollectorPayload.Context(timestamp, ip, userAgent, refererUri, headers, nuid)
      val source =
        CollectorPayload.Source(snowplowRawEvent.collector, snowplowRawEvent.encoding, hostname)
      // No way of storing API vendor/version in Thrift yet, assume Snowplow TP1
      CollectorPayload(CollectorPayload.SnowplowTp1, q, None, None, source, context).some
    }
  }

  private def parseNetworkUserId(
    str: String
  ): Either[FailureDetails.CPFormatViolationMessage, UUID] =
    Either
      .catchOnly[IllegalArgumentException](UUID.fromString(str))
      .leftMap(
        _ => CPFormatViolationMessage.InputData("networkUserId", Some(str), "not valid UUID")
      )
}
