/*
 * Copyright (c) 2012-2013 SnowPlow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.enrich.hadoop
package utils

// Java
import java.net.URLDecoder
import java.net.URI
import java.lang.{Integer => JInteger}
import java.lang.{Float => JFloat}

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Scalaz
import scalaz._
import Scalaz._

// Argonaut
import argonaut._
import Argonaut._

/**
 * General-purpose utils to help the
 * ETL process along.
 */
object ConversionUtils {

  /**
   * Simple case class wrapper around the
   * components of a URI.
   */
  case class UriComponents(
      // Required
      scheme: String,
      host: String,
      port: JInteger,
      // Optional
      path: Option[String],
      query: Option[String],
      fragment: Option[String])

  /**
   * Explodes a URI into its 6 components
   * pieces. Simple code but we use it
   * in multiple places
   *
   * @param uri The URI to explode into its
   *        constituent pieces
   *
   * @return The 6 components in a UriComponents
   *         case class
   */
  def explodeUri(uri: URI): UriComponents = {

    val port = uri.getPort

    // TODO: should we be using decodeString below instead?
    // Trouble is we can't be sure of the querystring's encoding.
    val query    = fixTabsNewlines(uri.getQuery)
    val path     = fixTabsNewlines(uri.getPath)
    val fragment = fixTabsNewlines(uri.getFragment)

    UriComponents(
      scheme   = uri.getScheme,
      host     = uri.getHost,
      port     = if (port == -1) 80 else port,
      path     = path,
      query    = query,
      fragment = fragment
      )
  }

  /**
   * A helper for the above
   * TODO: complete description
   */
  private def fixTabsNewlines(str: String): Option[String] = {
    val s = Option(str)
    val r = s.map(_.replaceAll("(\\r|\\n)", "")
             .replaceAll("\\t", "    "))
    if (r == Some("")) None else r
  }

  /**
   * Converts a JSON string into an \/[String, Json]
   *
   * @param json The JSON string to parse
   * @return a Scalaz \/, wrapping either a list of
   *         error Strings or the extracted Json
   */
  // TODO: change this so it returns a ValidationNel
  def extractJson(json: String): \/[String, Json] =
    Parse.parse(json)

  /**
   * Decodes a URL-safe Base64 string.
   *
   * For details on the Base 64 Encoding with URL
   * and Filename Safe Alphabet see:
   *
   * http://tools.ietf.org/html/rfc4648#page-7
   *
   * @param str The encoded string to be
   *            decoded
   * @param field The name of the field 
   * @return a Scalaz Validation, wrapping either an
   *         an error String or the decoded String
   */
  // TODO: probably better to change the functionality and signature
  // a little:
  //
  // 1. Signature -> : Validation[String, Option[String]]
  // 2. Functionality:
  //    1. If passed in null or "", return Success(None)
  //    2. If passed in a non-empty string but result == "", then return a Failure, because we have failed to decode something meaningful
  def decodeBase64Url(field: String, str: String): Validation[String, String] = {
    try {
      val decoder = new Base64(true) // true means "url safe"
      val decodedBytes = decoder.decode(str)
      val result = new String(decodedBytes)
      result.success
    } catch {
      case e =>
      "Field [%s]: exception Base64-decoding [%s] (URL-safe encoding): [%s]".format(field, str, e.getMessage).fail
    }
  }

  /**
   * Decodes a String in the specific encoding,
   * also removing:
   * * Newlines - because they will break Hive
   * * Tabs - because they will break non-Hive
   *          targets (e.g. Infobright)
   *
   * IMPLDIFF: note that this version, unlike
   * the Hive serde version, does not call
   * cleanUri. This is because we cannot assume
   * that str is a URI which needs 'cleaning'.
   *
   * TODO: simplify this when we move to a more
   * robust output format (e.g. Avro) - as then
   * no need to remove line breaks, tabs etc
   *
   * @param enc The encoding of the String
   * @param field The name of the field 
   * @param str The String to decode
   *
   * @return a Scalaz Validation, wrapping either
   *         an error String or the decoded String
   */
  val decodeString: (String, String, String) => Validation[String, String] = (enc, field, str) =>
    try {
      // TODO: switch to style of fixTabsNewlines above
      // TODO: potentially switch to using fixTabsNewlines too to avoid duplication
      val s = Option(str).getOrElse("")
      val d = URLDecoder.decode(s, enc)
      val r = d.replaceAll("(\\r|\\n)", "")
               .replaceAll("\\t", "    ")
      r.success
    } catch {
      case e =>
        "Field [%s]: Exception URL-decoding [%s] (encoding [%s]): [%s]".format(field, str, enc, e.getMessage).fail
    }

  /**
   * A wrapper around Java's
   * URI.create().
   *
   * Exceptions thrown by
   * URI.create():
   * 1. NullPointerException
   *    if uri is null
   * 2. IllegalArgumentException
   *    if uri violates RFC 2396
   *
   * @param uri The URI string to
   *        convert
   * @return an Option-boxed URI object, or an
   *         error message, all
   *         wrapped in a Validation
   */       
  def stringToUri(uri: String): Validation[String, Option[URI]] =
    try {
      Some(URI.create(uri)).success
    } catch {
      case e: NullPointerException => None.success
      case e: IllegalArgumentException => "Provided URI string [%s] violates RFC 2396: [%s]".format(uri, e.getMessage).fail
      case e => "Unexpected error creating URI from string [%s]: [%s]".format(uri, e.getMessage).fail
    }

  /**
   * Extract a Scala Int from
   * a String, or error.
   *
   * @param str The String
   *        which we hope is an
   *        Int
   * @param field The name of the
   *        field we are trying to
   *        process. To use in our
   *        error message
   * @return a Scalaz Validation,
   *         being either a
   *         Failure String or
   *         a Success JInt
   */
  val stringToJInteger: (String, String) => Validation[String, JInteger] = (field, str) =>
    try {
      val jint: JInteger = str.toInt
      jint.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Int".format(field, str).fail
    }

  /**
   * Extract a Scala Float from
   * a String, or error.
   *
   * @param str The String which we hope can
   *        be turned into a Float
   * @param field The name of the field
   *        we are trying to process. To use
   *        in our error message
   * @return a Scalaz Validation, being either
   *         a Failure String or a Success Int
   */
  val stringToJFloat: (String, String) => Validation[String, JFloat] = (field, str) =>
    try {
      if (str == "null") { // LEGACY. Yech, to handle a bug in the JavaScript tracker
        null.asInstanceOf[JFloat].success
      } else {
        val jfloat: JFloat = str.toFloat
        jfloat.success
      }
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Float".format(field, str).fail
    }

  /**
   * Extract a Scala Byte from
   * a String, or error.
   *
   * @param str The String
   *        which we hope is an
   *        Byte
   * @param field The name of the
   *        field we are trying to
   *        process. To use in our
   *        error message
   * @return a Scalaz Validation,
   *         being either a
   *         Failure String or
   *         a Success Byte
   */
  val stringToByte: (String, String) => Validation[String, Byte] = (field, str) =>
    try {
      str.toByte.success
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Byte".format(field, str).fail
    }

  /**
   * Converts a String of value "1" or "0"
   * to true or false respectively.
   *
   * @param str The String to convert
   * @return True for "1", false for "0", or
   *         an error message for any other
   *         value, all boxed in a Scalaz
   *         Validation
   */
  def stringToBoolean(str: String): Validation[String, Boolean] = 
    if (str == "1") {
      true.success
    } else if (str == "0") {
      false.success
    } else {
      "Cannot convert [%s] to boolean, only 1 or 0.".format(str).fail
    }

  /**
   * Truncates a String - useful for making sure
   * Strings can't overflow a database field.
   *
   * @param str The String to truncate
   * @param length The maximum length of the String
   *        to keep
   * @return the truncated String
   */
  def truncate(str: String, length: Int): String =
    if (str == null) {
      null
    } else {
      str.take(length)
    }

  /**
   * Helper to convert a Boolean value to a Byte.
   * Does not require any validation.
   *
   * @param bool The Boolean to convert into a Byte
   * @return 0 if false, 1 if true
   */
  def booleanToByte(bool: Boolean): Byte =
    if (bool) 1 else 0

  /**
   * Helper to convert a Byte value
   * (1 or 0) into a Boolean.
   *
   * @param b The Byte to turn
   *        into a Boolean
   * @return the Boolean value of b, or
   *         an error message if b is
   *         not 0 or 1 - all boxed in a
   *         Scalaz Validation 
   */
  def byteToBoolean(b: Byte): Validation[String, Boolean] =
    if (b == 0)
      false.success
    else if (b == 1)
      true.success
    else
      "Cannot convert byte [%s] to boolean, only 1 or 0.".format(b).fail
}