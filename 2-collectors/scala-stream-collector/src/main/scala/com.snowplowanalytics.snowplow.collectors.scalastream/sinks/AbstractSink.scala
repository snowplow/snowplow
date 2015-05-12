/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Java
import java.nio.ByteBuffer

// Thrift
import org.apache.thrift.TSerializer

// Logging
import org.slf4j.LoggerFactory

// Snowplow
import CollectorPayload.thrift.model1.CollectorPayload

// Define an interface for all sinks to use to store events.
trait AbstractSink {

  // Maximum number of bytes that a single record can contain
  val MaxBytes: Long

  lazy val log = LoggerFactory.getLogger(getClass())

  def storeRawEvent(event: CollectorPayload, key: String): List[Array[Byte]]

  // Serialize Thrift CollectorPayload objects
  private val thriftSerializer = new ThreadLocal[TSerializer] {
    override def initialValue = new TSerializer()
  }

  /**
   * If the CollectorPayload is too big to fit in a single record,
   * attempt to split it into multiple records
   *
   * @param event Incoming CollectorPayload
   * @return List of serialized CollectorPayloads
   */
  // TODO: send oversized unsplittable events to a bad sink
  def splitAndSerializePayload(event: CollectorPayload): List[Array[Byte]] = {

    // json4s - here because it causes a clash if imported at the outer level
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    import org.json4s._

    val serializer = thriftSerializer.get()

    val everythingSerialized = serializer.serialize(event)

    val wholeEventBytes = ByteBuffer.wrap(everythingSerialized).capacity

    // If the event is below the size limit, no splitting is necessary
    if (wholeEventBytes < MaxBytes) {
      List(everythingSerialized)
    } else {

      Option(parse(event.getBody)) match {

        // The record was presumably a GET
        case None => {
          log.error("Cannot split record with null body")
          Nil
        }

        // The record was a POST
        case Some(initialBody) => {

          val bodyDataArray = initialBody \ "data" match {
            case JNothing => None
            case data => Some(data)
          }

          val initialBodyDataBytes = ByteBuffer.wrap(compact(bodyDataArray).getBytes).capacity

          // If the event minus the data array is too big, splitting is hopeless
          if (wholeEventBytes - initialBodyDataBytes >= MaxBytes) {
            log.error("Even without the body, the serialized event is too large")
            Nil
          } else {

            val bodySchema = initialBody \ "schema"

            // The body array converted to a list of events
            val individualEvents: Option[List[String]] = for {
              d <- bodyDataArray
            } yield d.children.map(x => compact(x))

            val batchedIndividualEvents = individualEvents.map(SplitBatch.split(_, MaxBytes - wholeEventBytes + initialBodyDataBytes))

            batchedIndividualEvents match {

              case None => {
                log.error("Bad record with no data field")
                Nil
              }

              case Some(batches) => {

                batches.failedBigEvents foreach {
                  f => log.error(s"Failed event with body $f for being too large")
                }

                batches.goodBatches.map(batch => {

                  // Copy all data from the original payload into the smaller payloads
                  val payload = event.deepCopy()

                  payload.setBody(compact(
                    ("schema" -> bodySchema) ~
                    ("data" -> batch.map(evt => parse(evt)))))

                  serializer.serialize(payload)
                })
              }

            }
          }
        }
      }
    }
  }
}
