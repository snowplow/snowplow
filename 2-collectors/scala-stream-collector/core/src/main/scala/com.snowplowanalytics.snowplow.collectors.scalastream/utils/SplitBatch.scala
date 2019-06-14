/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream
package utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import cats.syntax.either._
import com.snowplowanalytics.iglu.core._
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.snowplow.badrows._
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import io.circe.Json
import io.circe.parser._
import io.circe.syntax._
import org.apache.thrift.TSerializer

import model._

/** Object handling splitting an array of strings correctly */
object SplitBatch {

  // Serialize Thrift CollectorPayload objects
  val ThriftSerializer = new ThreadLocal[TSerializer] {
    override def initialValue = new TSerializer()
  }

  /**
   * Split a list of strings into batches, none of them exceeding a given size
   * Input strings exceeding the given size end up in the failedBigEvents field of the result
   * @param input List of strings
   * @param maximum No good batch can exceed this size
   * @param joinSize Constant to add to the size of the string representing the additional comma
   *                 needed to join separate event JSONs in a single array
   * @return split batch containing list of good batches and list of events that were too big
   */
  def split(input: List[Json], maximum: Int, joinSize: Int = 1): SplitBatchResult = {
    @scala.annotation.tailrec
    def iterbatch(
      l: List[Json],
      currentBatch: List[Json],
      currentTotal: Long,
      acc: List[List[Json]],
      failedBigEvents: List[Json]
    ): SplitBatchResult = l match {
      case Nil => currentBatch match {
        case Nil => SplitBatchResult(acc, failedBigEvents)
        case nonemptyBatch => SplitBatchResult(nonemptyBatch :: acc, failedBigEvents)
      }
      case h :: t =>
        val headSize = getSize(h.noSpaces)
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
   * If the CollectorPayload is too big to fit in a single record, attempt to split it into
   * multiple records.
   * @param event Incoming CollectorPayload
   * @return a List of Good and Bad events
   */
  def splitAndSerializePayload(event: CollectorPayload, maxBytes: Int): EventSerializeResult = {
    val serializer = ThriftSerializer.get()
    val everythingSerialized = serializer.serialize(event)
    val wholeEventBytes = getSize(everythingSerialized)

    // If the event is below the size limit, no splitting is necessary
    if (wholeEventBytes < maxBytes) {
      EventSerializeResult(List(everythingSerialized), Nil)
    } else {
      (for {
        body <- Option(event.getBody).toRight("GET requests cannot be split")
        children <- splitBody(body)
        initialBodyDataBytes = getSize(Json.arr(children._2: _*).noSpaces)
        _ <- Either.cond[String, Unit](
          wholeEventBytes - initialBodyDataBytes < maxBytes,
          (),
          "cannot split this POST request because event without \"data\" field is still too big"
        )
        splitted = split(children._2, maxBytes - wholeEventBytes + initialBodyDataBytes)
        goodSerialized = serializeBatch(serializer, event, splitted.goodBatches, children._1)
        badList = splitted.failedBigEvents.map { e =>
          val msg = "this POST request split is still too large"
          oversizedPayload(event, getSize(e), maxBytes, msg)
        }
      } yield EventSerializeResult(goodSerialized, badList)).fold({
        msg =>
          val tooBigPayload = oversizedPayload(event, wholeEventBytes, maxBytes, msg)
          EventSerializeResult(Nil, List(tooBigPayload))
        },
        identity
      )
    }
  }

  def splitBody(body: String): Either[String, (SchemaKey, List[Json])] = for {
    json <- parse(body)
      .leftMap(e => s"cannot split POST requests which are not json ${e.getMessage}")
    sdd <- json.as[SelfDescribingData[Json]]
      .leftMap(e => s"cannot split POST requests which are not self-describing ${e.getMessage}")
    array <- sdd.data.asArray
      .toRight("cannot split POST requests which do not contain a data array")
  } yield (sdd.schema, array.toList)

  /**
   * Creates a bad row while maintaining a truncation of the original payload to ease debugging.
   * Keeps a tenth of the original payload.
   * @param event original payload
   * @param size size of the oversized payload
   * @param maxSize maximum size allowed
   * @param msg error message
   * @return the created bad rows as json
   */
  private def oversizedPayload(
    event: CollectorPayload,
    size: Int,
    maxSize: Int,
    msg: String
  ): Array[Byte] =
    BadRow.SizeViolation(
      Processor(generated.BuildInfo.name, generated.BuildInfo.version),
      Failure.SizeViolation(Instant.now(), maxSize, size, s"oversized collector payload: $msg"),
      Payload.RawPayload(event.toString().take(maxSize / 10))
    ).asJson.noSpaces.getBytes(UTF_8)

  private def getSize(a: Array[Byte]): Int = ByteBuffer.wrap(a).capacity

  private def getSize(s: String): Int = getSize(s.getBytes(UTF_8))

  private def getSize(j: Json): Int = getSize(j.noSpaces)

  private def serializeBatch(
    serializer: TSerializer,
    event: CollectorPayload,
    batches: List[List[Json]],
    schema: SchemaKey
  ): List[Array[Byte]] =
    batches.map { batch =>
      val payload = event.deepCopy()
      val body = SelfDescribingData[Json](schema, Json.arr(batch: _*))
      payload.setBody(body.asJson.noSpaces)
      serializer.serialize(payload)
    }
}
