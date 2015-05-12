/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package collectors
package scalastream
package sinks

// Java
import java.nio.ByteBuffer

/**
 * Object handling splitting an array of strings correctly
 */
object SplitBatch {

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
  def split(input: List[String], maximum: Long, joinSize: Long = 1): SplitBatch = {

    import scala.annotation.tailrec
    @tailrec
    def iterbatch(
      l: List[String],
      currentBatch: List[String],
      currentTotal: Long,
      acc: List[List[String]],
      failedBigEvents: List[String]): SplitBatch = l match {

      case Nil => currentBatch match {
        case Nil => SplitBatch(acc, failedBigEvents)
        case nonemptyBatch => SplitBatch(nonemptyBatch :: acc, failedBigEvents)
      }

      case h :: t => {
        val headSize = ByteBuffer.wrap(h.getBytes).capacity
        if (headSize + joinSize > maximum) {
          iterbatch(t, currentBatch, currentTotal, acc, h :: failedBigEvents)
        } else if (headSize + currentTotal + joinSize > maximum) {
          iterbatch(l, Nil, 0,  currentBatch :: acc, failedBigEvents)
        } else {
          iterbatch(t, h :: currentBatch, headSize + currentTotal + joinSize, acc, failedBigEvents)
        }
      }
    }

    iterbatch(input, Nil, 0, Nil, Nil)
  }
}

/**
 * Class for the result of splitting a too-large array of events in the body of a POST request
 *
 * @param goodBatches List of batches of events
 * @param failedBigEvents List of events that were too large
 */
case class SplitBatch(goodBatches: List[List[String]], failedBigEvents: List[String])
