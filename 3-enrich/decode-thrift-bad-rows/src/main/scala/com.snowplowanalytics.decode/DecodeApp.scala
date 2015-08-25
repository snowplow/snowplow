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
package com.snowplowanalytics.decode

import org.apache.commons.codec.binary.Base64
import org.apache.thrift.TDeserializer
import com.snowplowanalytics.snowplow.CollectorPayload.thrift.model1.CollectorPayload
import scala.io.Source
import scala.util.control.NonFatal

object DecodeApp {
  def main(args: Array[String]) {

    Source.stdin.getLines foreach { line => try {
        println(decode(line))
      } catch {
        case NonFatal(e) => Console.err.println(e)
      }
    }
  }

  private def decode(s: String): String = {
    val decoded = Base64.decodeBase64(s)
    val deserialized = new CollectorPayload
    new TDeserializer().deserialize(deserialized, decoded)
    deserialized.toString
  }
}
