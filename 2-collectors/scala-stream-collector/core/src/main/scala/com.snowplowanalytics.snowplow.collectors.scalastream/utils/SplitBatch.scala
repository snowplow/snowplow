/*
 * Copyright (c) 2013-2017 Snowplow Analytics Ltd. All rights reserved.
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
package snowplow
package collectors
package scalastream
package utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.control.NonFatal

import org.apache.thrift.TSerializer
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import scalaz._

import CollectorPayload.thrift.model1.CollectorPayload
import enrich.common.outputs.BadRow
import iglu.client.validation.ProcessingMessageMethods._
import model._

/**
 * Object handling splitting an array of strings correctly
 */
object SplitBatch {

  // Serialize Thrift CollectorPayload objects
  val ThriftSerializer = new ThreadLocal[TSerializer] {
    override def initialValue = new TSerializer()
  }

  // An ISO valid timestamp formatter
  private val TstampFormat = DateTimeFormat
    .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    .withZone(DateTimeZone.UTC)

  // json4s
  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._

  /**
   * Split a list of strings into batches, none of them exceeding a given size
   * Input strings exceeding the given size end up in the failedBigEvents field of the result
   *
   * @param input List of strings
   * @param maximum No good batch can exceed this size
   * @param joinSize Constant to add to the size of the string representing the additional comma
   *                 needed to join separate event JSONs in a single array
   * @return split batch containing list of good batches and list of events that were too big
   */
  def split(input: List[String], maximum: Long, joinSize: Long = 1): SplitBatchResult = {

    import scala.annotation.tailrec
    @tailrec
    def iterbatch(
      l: List[String],
      currentBatch: List[String],
      currentTotal: Long,
      acc: List[List[String]],
      failedBigEvents: List[String]): SplitBatchResult = l match {

      case Nil => currentBatch match {
        case Nil => SplitBatchResult(acc, failedBigEvents)
        case nonemptyBatch => SplitBatchResult(nonemptyBatch :: acc, failedBigEvents)
      }
      case h :: t =>
        val headSize = ByteBuffer.wrap(h.getBytes(UTF_8)).capacity
        if (headSize + joinSize > maximum) {
          iterbatch(t, currentBatch, currentTotal, acc, h :: failedBigEvents)
        } else if (headSize + currentTotal + joinSize > maximum) {
          iterbatch(l, Nil, 0, currentBatch :: acc, failedBigEvents)
        } else {
          iterbatch(t, h :: currentBatch, headSize + currentTotal + joinSize, acc, failedBigEvents)
        }
    }

    iterbatch(input, Nil, 0, Nil, Nil)
  }

  /**
   * If the CollectorPayload is too big to fit
   * in a single record, attempt to split it into
   * multiple records.
   *
   * @param event Incoming CollectorPayload
   * @return a List of Good and Bad events
   */
  def splitAndSerializePayload(event: CollectorPayload, maxBytes: Long): EventSerializeResult = {

    val serializer = ThriftSerializer.get()
    val everythingSerialized = serializer.serialize(event)
    val wholeEventBytes = ByteBuffer.wrap(everythingSerialized).capacity.toLong

    // If the event is below the size limit, no splitting is necessary
    if (wholeEventBytes < maxBytes) {
      EventSerializeResult(List(everythingSerialized), Nil)
    } else {
      event.getBody match {
        case null =>
          // Event was a GET
          val err = "cannot split record with null body"
          val payload = oversizedPayload(event, wholeEventBytes, maxBytes, err)
          EventSerializeResult(Nil, List(payload))
        case body =>
          try {
            // Try to parse the body
            val initialBody = parse(body)

            // Event was a POST
            val bodyDataArray = initialBody \ "data" match {
              case JNothing => None
              case data => Some(data)
            }

            val initialBodyDataBytes =
              ByteBuffer.wrap(compact(bodyDataArray).getBytes(UTF_8)).capacity

            // If the event minus the data array is too big, splitting is hopeless
            if (wholeEventBytes - initialBodyDataBytes >= maxBytes) {
              val err = "event without \"data\" field is still too big"
              val payload = oversizedPayload(event, wholeEventBytes, maxBytes, err)
              EventSerializeResult(Nil, List(payload))
            } else {

              val bodySchema = initialBody \ "schema"

              // The body array converted to a list of events
              val individualEvents: Option[List[String]] =
                bodyDataArray.map(b => b.children.map(compact))

              val batchedIndividualEvents = individualEvents.map(
                split(_, maxBytes - wholeEventBytes + initialBodyDataBytes))

              batchedIndividualEvents match {
                case None =>
                  val err = "record has no data field"
                  val payload = oversizedPayload(event, wholeEventBytes, maxBytes, err)
                  EventSerializeResult(Nil, List(payload))
                case Some(batches) =>
                  // Copy all data from the original payload into the smaller payloads
                  val goodList = batches.goodBatches.map { batch =>
                    val payload = event.deepCopy()
                    val data = batch.map(evt => parse(evt))
                    val body = getGoodRow(bodySchema, data)
                    payload.setBody(body)
                    serializer.serialize(payload)
                  }

                  val badList = batches.failedBigEvents.map { e =>
                    val size = ByteBuffer.wrap(e.getBytes(UTF_8)).capacity.toLong
                    val err = "one of the split event is still too large"
                    oversizedPayload(event, size, maxBytes, err)
                  }

                  // Return Good and Bad Lists
                  EventSerializeResult(goodList, badList)
              }
            }
          } catch {
            case NonFatal(e) =>
              val err = s"could not parse payload body ${e.getMessage}"
              val payload = oversizedPayload(event, wholeEventBytes, maxBytes, err)
              EventSerializeResult(Nil, List(payload))
          }
      }
    }
  }

  /**
   * Creates a bad row while maintaining a truncation of the original payload to ease debugging.
   * Keeps a tenth of the original payload.
   * @param event original payload
   * @param size size of the oversized payload
   * @param maxSize maximum size allowed
   * @param errorMessage additional context for the fact that the payload is too big
   * @return the created bad rows as json
   */
  private def oversizedPayload(
      event: CollectorPayload, size: Long, maxSize: Long, errorMessage: String): Array[Byte] = {
    val err = s"Oversized payload, size: $size, max size: $maxSize, reason: $errorMessage"
      .toProcessingMessage
    BadRow(event.toString.take((maxSize / 10L).intValue), NonEmptyList(err))
      .toCompactJson
      .getBytes(UTF_8)
  }

  /**
   * Returns a Good Row as a String
   *
   * @param schema The schema for this event
   * @param data A List of JValues to embed for this event
   */
  private def getGoodRow(schema: JValue, data: List[JValue]): String = {
    compact(
      ("schema" -> schema) ~
      ("data" -> data)
    )
  }

  /**
   * Returns an ISO valid timestamp
   *
   * @param tstamp The Timestamp to convert
   * @return the formatted Timestamp
   */
  private def getTimestamp(tstamp: Long): String = {
    val dt = new DateTime(tstamp)
    TstampFormat.print(dt)
  }
}
