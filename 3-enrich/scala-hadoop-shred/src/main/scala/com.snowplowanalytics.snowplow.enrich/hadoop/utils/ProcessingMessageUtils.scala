/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich
package hadoop
package utils

import com.github.fge.jsonschema.core.report.{
  ProcessingMessage,
  LogLevel
}

// Scalaz
import scalaz._
import Scalaz._

object ProcessingMessageUtils {

  /**
   * Implicit to pimp a Scalaz Validation to a
   * version that makes it easy to convert
   * Failure Strings to Failure ProcessingMessages.
   *
   * @param instance A JsonNode
   * @return the pimped ValidatableJsonNode
   */
  implicit def pimpValidation[A](validation: Validation[String, A]) = new ProcMsgValidation[A](validation)

  /**
   * A helper method to convert a String into
   * a ProcessingMessage. Assume that the
   * String is coming from a Validation's
   * Failure, so the LogLevel is ERROR.
   *
   * @param message The error message or other
   *        String for our ProcessingMessage
   * @param logLevel The log level for this
   *        ProcessingMessage. Defaults to
   *        ERROR
   * @return the generated ProcessingMessage
   *         with message and log level set
   */
  def toProcMsg(message: String, logLevel: LogLevel = LogLevel.ERROR): ProcessingMessage =
    new ProcessingMessage()
      .setLogLevel(logLevel)
      .setMessage(message)


  def toProcMsgNel(message: String, logLevel: LogLevel = LogLevel.ERROR): NonEmptyList[ProcessingMessage] =
    NonEmptyList(toProcMsg(message, logLevel))

}

class ProcMsgValidation[+A](validation: Validation[String, A]) {

  def toProcessingMessage: Validation[ProcessingMessage, A] =
    validation.leftMap { err =>
      ProcessingMessageUtils.toProcMsg(err, LogLevel.ERROR)
    }

  def toProcessingMessageNel: ValidationNel[ProcessingMessage, A] =
    toProcessingMessage.toValidationNel
}
