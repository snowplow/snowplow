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
package com.snowplowanalytics.snowplow.enrich.common
package inputs

// Apache URLEncodedUtils
import org.apache.http.message.BasicNameValuePair

// Scalaz
import scalaz._
import Scalaz._

object LoaderSpecHelpers {

  private type NvPair = Tuple2[String, String]

  /**
   * Converts an NvPair into a
   * BasicNameValuePair
   *
   * @param pair The Tuple2[String, String] name-value
   * pair to convert
   * @return the basic name value pair 
   */
  private def toNvPair(pair: NvPair): BasicNameValuePair =
    new BasicNameValuePair(pair._1, pair._2)

  /**
   * Converts the supplied NvPairs into a 
   * a NameValueNel.
   *
   * @param head The first NvPair to convert
   * @param tail The rest of the NvPairs to
   * convert
   * @return the populated NvGetPayload
   */
  def toNameValueNel(head: NvPair, tail: NvPair*): NameValueNel =
    NonEmptyList(toNvPair(head), tail.map(toNvPair(_)): _*)

  /**
   * Converts the supplied NvPairs into an
   * NvGetPayload. See above for NvPair
   * definition.
   *
   * @param head The first NvPair to convert
   * @param tail The rest of the NvPairs to
   * convert
   * @return the populated NvGetPayload
   */
  def toPayload(head: NvPair, tail: NvPair*): NvGetPayload =
    new NvGetPayload(toNameValueNel(head, tail: _*))
}
