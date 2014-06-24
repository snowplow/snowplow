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
  MappingException
}
import org.json4s.JsonDSL._

// Iglu
import iglu.client.validation.ProcessingMessageMethods._

object ScalazJson4sUtils {

  implicit val formats = DefaultFormats

  /**
   * Returns a String field at the end of a JSON path.
   *
   * @param JValue The JSON from which the String is
   *        to be extracted
   * @param field NonEmptyList containing the Strings
   *        which make up the path
   * @return the String extracted from the JSON on success,
   *         or an error String on failure
   */
  def extractString(config: JValue, field: NonEmptyList[String]): ValidatedMessage[String] =
    
    try {
      field.foldLeft(config)(_ \ _).extract[String].success
    } catch {
      case me: MappingException => s"Could not extract %s as String from supplied JSON".format(field.toList.mkString(".")).toProcessingMessage.fail
    }

  /**
   * Returns an Int field at the end of a JSON path.
   *
   * @param JValue The JSON from which the Int is
   *        to be extracted
   * @param field NonEmptyList containing the Strings
   *        which make up the path
   * @return the Int extracted from the JSON on success,
   *         or an error String on failure
   */
  def extractInt(config: JValue, field: NonEmptyList[String]): ValidatedMessage[Int] =
    try {
      field.foldLeft(config)(_ \ _).extract[Int].success 
    } catch {
      case me: MappingException => s"Could not extract %s as Int from supplied JSON".format(field.toList.mkString(".")).toProcessingMessage.fail
    }

  /**
   * Returns a Boolean field at the end of a JSON path.
   *
   * @param JValue The JSON from which the Boolean is
   *        to be extracted
   * @param field NonEmptyList containing the Strings
   *        which make up the path
   * @return the Boolean extracted from the JSON on
   *         success or an error String on failure
   */
  def extractBoolean(config: JValue, field: NonEmptyList[String]): ValidatedMessage[Boolean] =
    try {
      field.foldLeft(config)(_ \ _).extract[Boolean].success 
    } catch {
      case me: MappingException => s"Could not extract %s as Boolean from supplied JSON".format(field.toList.mkString(".")).toProcessingMessage.fail
    }    
}
