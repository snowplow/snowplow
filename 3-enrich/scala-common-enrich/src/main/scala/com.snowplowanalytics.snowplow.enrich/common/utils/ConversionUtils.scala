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
package utils

// Java
import java.net.URI
import java.net.URLDecoder
import java.net.URLEncoder
import java.lang.{Integer => JInteger}
import java.math.{BigDecimal => JBigDecimal}
import java.lang.{Byte => JByte}
import java.util.UUID
import java.nio.charset.StandardCharsets.UTF_8

// Scala
import scala.util.Try
import scala.util.control.NonFatal
import scala.collection.JavaConversions._

// Apache HTTP
import org.apache.http.client.utils.URLEncodedUtils

// Apache Commons
import org.apache.commons.lang3.exception.ExceptionUtils

// Apache Commons Codec
import org.apache.commons.codec.binary.Base64

// Scalaz
import scalaz._
import Scalaz._

// Scala URI
import com.netaporter.uri.Uri

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
    val query    = fixTabsNewlines(uri.getRawQuery)
    val path     = fixTabsNewlines(uri.getRawPath)
    val fragment = fixTabsNewlines(uri.getRawFragment)

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
   * Quick helper to make sure our Strings are TSV-safe,
   * i.e. don't include tabs, special characters, newlines
   * etc.
   *
   * @param str The string we want to make safe
   * @return a safe String
   */
  def makeTsvSafe(str: String): String =
    fixTabsNewlines(str).orNull

  /**
   * Replaces tabs with four spaces and removes
   * newlines altogether.
   *
   * Useful to prepare user-created strings for
   * fragile storage formats like TSV.
   *
   * @param str The String to fix

   * @return The String with tabs and newlines fixed.
   */
  def fixTabsNewlines(str: String): Option[String] = {
    val f = for {
      s <- Option(str)
      r = s.replaceAll("\\t", "    ")
           .replaceAll("\\p{Cntrl}", "") // Any other control character
    } yield r
    if (f == Some("")) None else f
  }

  /**
   * Decodes a URL-safe Base64 string.
   *
   * For details on the Base 64 Encoding with URL
   * and Filename Safe Alphabet see:
   *
   * http://tools.ietf.org/html/rfc4648#page-7
   *
   * @param str The encoded string to be
   * decoded
   * @param field The name of the field
   * @return a Scalaz Validation, wrapping either an
   * an error String or the decoded String
   */
   // TODO: probably better to change the functionality and signature
   // a little:
   //
   // 1. Signature -> : Validation[String, Option[String]]
   // 2. Functionality:
   // 1. If passed in null or "", return Success(None)
   // 2. If passed in a non-empty string but result == "", then return a Failure, because we have failed to decode something meaningful
  def decodeBase64Url(field: String, str: String): Validation[String, String] = {
    try {
      val decoder = new Base64(true) // true means "url safe"
      val decodedBytes = decoder.decode(str)
      val result = new String(decodedBytes, UTF_8) // Must specify charset (EMR uses US_ASCII)
      result.success
    } catch {
      case e =>
      "Field [%s]: exception Base64-decoding [%s] (URL-safe encoding): [%s]".format(field, str, e.getMessage).fail
    }
  }

  /**
   * Validates that the given field contains a valid UUID.
   *
   * @param field The name of the field being validated
   * @param str The String hopefully containing a UUID
   * @return a Scalaz ValidatedString containing either
   *         the original String on Success, or an error
   *         String on Failure.
   */
  val validateUuid: (String, String) => ValidatedString = (field, str) => {

    def check(s: String)(u: UUID): Boolean = (u != null && s.toLowerCase == u.toString)
    val uuid = Try(UUID.fromString(str)).toOption.filter(check(str))
    uuid match {
      case Some(_) => str.toLowerCase.success
      case None    => s"Field [$field]: [$str] is not a valid UUID".fail
    }
  } 

  /**
   * @param field The name of the field being validated
   * @param str The String hopefully parseable as an integer
   * @return a Scalaz ValidatedString containing either
   *         the original String on Success, or an error
   *         String on Failure.
   */
  val validateInteger: (String, String) => ValidatedString = (field, str) => {
    try {
      str.toInt
      str.success
    } catch {
      case _ : java.lang.NumberFormatException => s"Field [$field]: [$str] is not a valid integer".fail
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
  val decodeString: (String, String, String) => ValidatedString = (enc, field, str) =>
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
     * On 17th August 2013, Amazon made an
     * unannounced change to their CloudFront
     * log format - they went from always encoding
     * % characters, to only encoding % characters
     * which were not previously encoded. For a
     * full discussion of this see:
     *
     * https://forums.aws.amazon.com/thread.jspa?threadID=134017&tstart=0#
     *
     * On 14th September 2013, Amazon rolled out a further fix,
     * from which point onwards all fields, including the
     * referer and useragent, would have %s double-encoded.
     *
     * This causes issues, because the ETL process expects
     * referers and useragents to be only single-encoded.
     *
     * This function turns a double-encoded percent (%) into
     * a single-encoded one.
     *
     * Examples:
     * 1. "page=Celestial%25Tarot"          -   no change (only single encoded)
     * 2. "page=Dreaming%2520Way%2520Tarot" -> "page=Dreaming%20Way%20Tarot"
     * 3. "loading 30%2525 complete"        -> "loading 30%25 complete"
     *
     * Limitation of this approach: %2588 is ambiguous. Is it a:
     * a) A double-escaped caret "ˆ" (%2588 -> %88 -> ^), or:
     * b) A single-escaped "%88" (%2588 -> %88)
     *
     * This code assumes it's a).
     *
     * @param str The String which potentially has double-encoded %s
     * @return the String with %s now single-encoded
     */
    def singleEncodePcts(str: String): String =
      str.replaceAll("%25([0-9a-fA-F][0-9a-fA-F])", "%$1") // Decode %25XX to %XX

    /**
     * Decode double-encoded percents, then percent decode
     *
     * @param field The name of the field 
     * @param str The String to decode
     *
     * @return a Scalaz Validation, wrapping either
     *         an error String or the decoded String
     */
    def doubleDecode(field: String, str: String): ValidatedString =
      ConversionUtils.decodeString("UTF-8", field, singleEncodePcts(str))

  /**
   * Encodes a string in the specified encoding
   *
   * @param enc The encoding to be used
   * @param str The string which needs to be URLEncoded
   * @return a URL encoded string
   */
  def encodeString(enc: String, str: String): String =
    URLEncoder.encode(str, enc)

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
   * @param useNetaporter Whether to use the
   *        com.netaporter.uri library
   * @return an Option-boxed URI object, or an
   *         error message, all
   *         wrapped in a Validation
   */
  def stringToUri(uri: String, useNetaporter: Boolean = false): Validation[String, Option[URI]] =
    try {
      val r = uri.replaceAll(" ", "%20") // Because so many raw URIs are bad, #346
      Some(URI.create(r)).success
    } catch {
      case npe: NullPointerException => None.success
      case iae: IllegalArgumentException => useNetaporter match {
        case false => {
          val netaporterUri = try {
            Uri.parse(uri).success
          } catch {
            case e => "Provided URI string [%s] could not be parsed by Netaporter: [%s]".format(uri, ExceptionUtils.getRootCause(iae).getMessage).fail
          }
          for {
            parsedUri <- netaporterUri
            finalUri <- stringToUri(parsedUri.toString, true)
          } yield finalUri
        }
        case true => "Provided URI string [%s] violates RFC 2396: [%s]".format(uri, ExceptionUtils.getRootCause(iae).getMessage).fail
      }
      case e => "Unexpected error creating URI from string [%s]: [%s]".format(uri, e.getMessage).fail
    }

  /**
   * Attempt to extract the querystring from a URI as a map
   *
   * @param uri URI containing the querystring
   * @param encoding Encoding of the URI
   */
  def extractQuerystring(uri: URI, encoding: String): Validation[String, Map[String, String]] =
    try {
      URLEncodedUtils.parse(uri, encoding).map(p => (p.getName -> p.getValue)).toMap.success
    } catch {
      case NonFatal(e1) => try {
        Uri.parse(uri.toString).query.params.toMap.success
      } catch {
        case NonFatal(e2) =>
          s"Could not parse uri [$uri]. Apache Httpclient threw exception: [$e1]. Net-a-porter threw exception: [$e2]".fail
      }
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
    if (Option(str).isEmpty) {
      null.asInstanceOf[JInteger].success
    } else {
      try {
        val jint: JInteger = str.toInt
        jint.success
      } catch {
        case nfe: NumberFormatException =>
          "Field [%s]: cannot convert [%s] to Int".format(field, str).fail
      }
    }

  /**
   * Convert a String to a String containing a
   * Redshift-compatible Double.
   *
   * Necessary because Redshift does not support all
   * Java Double syntaxes e.g. "3.4028235E38"
   *
   * Note that this code does NOT check that the
   * value will fit within a Redshift Double -
   * meaning Redshift may silently round this number
   * on load.
   *
   * @param str The String which we hope contains
   *        a Double
   * @param field The name of the field we are
   *        validating. To use in our error message
   * @return a Scalaz Validation, being either
   *         a Failure String or a Success String
   */
  val stringToDoublelike: (String, String) => ValidatedString = (field, str) =>
    try {
      if (Option(str).isEmpty || str == "null") { // "null" String check is LEGACY to handle a bug in the JavaScript tracker
        null.asInstanceOf[String].success
      } else {
        val jbigdec = new JBigDecimal(str)
        jbigdec.toPlainString.success // Strip scientific notation
      }
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Double-like String".format(field, str).fail
    }

  /**
   * Convert a String to a Double
   *
   * @param str The String which we hope contains
   *        a Double
   * @param field The name of the field we are
   *        validating. To use in our error message
   * @return a Scalaz Validation, being either
   *         a Failure String or a Success Double
   */
  def stringToMaybeDouble(field: String, str: String): Validation[String, Option[Double]] = {
    try {
      if (Option(str).isEmpty || str == "null") { // "null" String check is LEGACY to handle a bug in the JavaScript tracker
        None.success
      } else {
        val jbigdec = new JBigDecimal(str)
        jbigdec.doubleValue().some.success
      }
    } catch {
     case nfe: NumberFormatException =>
        "Field [%s]: cannot convert [%s] to Double-like String".format(field, str).fail
    }
  }

  /**
   * Extract a Java Byte representing
   * 1 or 0 only from a String, or error.
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
  val stringToBooleanlikeJByte: (String, String) => Validation[String, JByte] = (field, str) =>
    str match {
      case "1" => (1.toByte: JByte).success
      case "0" => (0.toByte: JByte).success
      case _   => "Field [%s]: cannot convert [%s] to Boolean-like JByte".format(field, str).fail
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
  def booleanToJByte(bool: Boolean): JByte =
    (if (bool) 1 else 0).toByte

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
