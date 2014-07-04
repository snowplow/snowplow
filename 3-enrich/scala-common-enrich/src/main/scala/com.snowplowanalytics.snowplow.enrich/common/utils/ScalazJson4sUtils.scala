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
package snowplow
package enrich
package common
package utils

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s.{
  DefaultFormats,
  JValue,
  JString,
  MappingException
}
import org.json4s.JsonDSL._

// Iglu
import iglu.client.validation.ProcessingMessageMethods._

object ScalazJson4sUtils {

  implicit val formats = DefaultFormats

  /**
   * Returns a field of type A at the end of a
   * JSON path
   *
   * @tparam A Type of the field to extract
   * @param head The first field in the JSON path
   *        Exists to ensure the path is nonempty
   * @param tail The rest of the fields in the
   *        JSON path
   * @return the list extracted from the JSON on
   *         success or an error String on failure
   */
  def extract[A: Manifest](config: JValue, head: String, tail: String*): ValidatedMessage[A] = {
    
    val path = head +: tail

    try {
      path.foldLeft(config)(_ \ _).extract[A].success
    } catch {
      case me: MappingException => s"Could not extract %s as %s from supplied JSON".format(path.mkString("."), manifest[A]).toProcessingMessage.fail
    }
  }
}
