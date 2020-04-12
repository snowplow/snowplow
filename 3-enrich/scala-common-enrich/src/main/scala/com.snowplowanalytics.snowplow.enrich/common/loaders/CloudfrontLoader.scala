/*
 * Copyright (c) 2012-2020 Snowplow Analytics Ltd. All rights reserved.
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
package loaders

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import scala.util.matching.Regex

import cats.data.ValidatedNel
import cats.implicits._
import com.snowplowanalytics.snowplow.badrows._

import utils.ConversionUtils.singleEncodePcts

/**
 * The dedicated loader for events collected by CloudFront.
 * We support the following CloudFront access log formats:
 * 1. Pre-12 Sep 2012
 * 2. 12 Sep 2012 - 21 Oct 2013
 * 3. 21 Oct 2013 - 29 Apr 2014
 * 4. Potential future updates, provided they are solely additive in nature
 * For more details on this format, please see:
 * http://docs.amazonwebservices.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html#LogFileFormat
 */
object CloudfrontLoader extends Loader[String] {
  // The encoding used on CloudFront logs
  private val CollectorEncoding = UTF_8

  // The name of this collector
  private val CollectorName = "cloudfront"

  private val originalFields = List(
    "([\\S]+)", // Date          / date
    "([\\S]+)", // Time          / time
    "([\\S]+)", // EdgeLocation  / x-edge-location
    "([\\S]+)", // BytesSent     / sc-bytes
    "([\\S]+)", // IPAddress     / c-ip
    "([\\S]+)", // Operation     / cs-method
    "([\\S]+)", // Domain        / cs(Host)
    "([\\S]+)", // Object        / cs-uri-stem
    "([\\S]+)", // HttpStatus    / sc-status
    "([\\S]+)", // Referer       / cs(Referer)
    "([\\S]+)", // UserAgent     / cs(User Agent)
    "([\\S]+)" // Querystring   / cs-uri-query
  )
  private val fields12Sep2012 = originalFields ++ List(
    "[\\S]*", // CookieHeader  / cs(Cookie)         added 12 Sep 2012 // TODO: why the *?
    "[\\S]+", // ResultType    / x-edge-result-type added 12 Sep 2012
    "[\\S]+" // X-Amz-Cf-Id   / x-edge-request-id  added 12 Sep 2012
  )
  private val fields21Oct2013 = fields12Sep2012 ++ List(
    "[\\S]+", // XHostHeader   / x-host-header      added 21 Oct 2013
    "[\\S]+", // CsProtocol    / cs-protocol        added 21 Oct 2013
    "[\\S]+" // CsBytes       / cs-bytes           added 21 Oct 2013
  )
  private val fields29Apr2014 = fields21Oct2013 ++ List(
    "[\\S]+" // TimeTaken     / time-taken         added 29 Apr 2014
  )
  private val fields01Jul2014 = fields29Apr2014 ++ List(
    "([\\S]+)", // ForwardedFor  / x-forwarded-for             added 01 Jul 2014
    "[\\S]+", // SslProtocol   / ssl-protocol                added 01 Jul 2014
    "[\\S]+", // SslCipher     / ssl-cipher                  added 01 Jul 2014
    "[\\S]+" // EdgeResResult / x-edge-response-result-type added 01 Jul 2014
  )

  private val CfOriginalPlusAdditionalRegex = toRegex(originalFields, additionalFields = true)
  private val CfOriginalRegex = toRegex(originalFields)
  private val Cf12Sep2012Regex = toRegex(fields12Sep2012)
  private val Cf21Oct2013Regex = toRegex(fields21Oct2013)
  private val Cf29Apr2014Regex = toRegex(fields29Apr2014)
  private val Cf01Jul2014Regex = toRegex(fields01Jul2014, additionalFields = true)

  /**
   * Converts the source string into a ValidatedMaybeCollectorPayload.
   * @param line A line of data to convert
   * @return either a set of validation errors or an Option-boxed CanonicalInput object, wrapped
   * in a ValidatedNel.
   */
  override def toCollectorPayload(line: String, processor: Processor): ValidatedNel[BadRow.CPFormatViolation, Option[CollectorPayload]] =
    (line match {
      // 1. Header row
      case h if h.startsWith("#Version:") || h.startsWith("#Fields:") => None.valid
      // 2. Not a GET request
      case CfOriginalPlusAdditionalRegex(_, _, _, _, _, op, _, _, _, _, _, _) if op.toUpperCase != "GET" =>
        val msg = "operation must be GET"
        FailureDetails.CPFormatViolationMessage
          .InputData("verb", op.toUpperCase().some, msg)
          .invalidNel
      // 3. Row matches original CloudFront format
      case CfOriginalRegex(date, time, _, _, ip, _, _, objct, _, rfr, ua, qs) =>
        CloudfrontLogLine(date, time, ip, objct, rfr, ua, qs).toValidatedMaybeCollectorPayload
      case Cf12Sep2012Regex(date, time, _, _, ip, _, _, objct, _, rfr, ua, qs) =>
        CloudfrontLogLine(date, time, ip, objct, rfr, ua, qs).toValidatedMaybeCollectorPayload
      case Cf21Oct2013Regex(date, time, _, _, ip, _, _, objct, _, rfr, ua, qs) =>
        CloudfrontLogLine(date, time, ip, objct, rfr, ua, qs).toValidatedMaybeCollectorPayload
      case Cf29Apr2014Regex(date, time, _, _, ip, _, _, objct, _, rfr, ua, qs) =>
        CloudfrontLogLine(date, time, ip, objct, rfr, ua, qs).toValidatedMaybeCollectorPayload
      case Cf01Jul2014Regex(date, time, _, _, ip, _, _, objct, _, rfr, ua, qs, forwardedFor) =>
        CloudfrontLogLine(date, time, ip, objct, rfr, ua, qs, forwardedFor).toValidatedMaybeCollectorPayload
      // 4. Row not recognised
      case _ =>
        FailureDetails.CPFormatViolationMessage
          .Fallback("does not match header or data row formats")
          .invalidNel
    }).leftMap(
      _.map(
        f =>
          BadRow.CPFormatViolation(
            processor,
            Failure.CPFormatViolation(Instant.now(), CollectorName, f),
            Payload.RawPayload(line)
          )
      )
    )

  /**
   * 'Cleans' a string to make it parsable by URLDecoder.decode.
   * The '%' character seems to be appended to the end of some URLs in the CloudFront logs, causing
   * Exceptions when using URLDecoder.decode. Perhaps a CloudFront bug?
   * @param uri The String to clean
   * @return the cleaned string
   */
  private[loaders] def toCleanUri(uri: String): String =
    uri
      .foldLeft((new StringBuilder, 1)) {
        case ((acc, cnt), c) =>
          if (cnt == uri.length() && c == '%') (acc, cnt)
          else (acc.append(c), cnt + 1)
      }
      ._1
      .toString()

  private def toRegex(fields: List[String], additionalFields: Boolean = false): Regex = {
    val whitespaceRegex = "[\\s]+"
    if (additionalFields)
      fields.mkString("", whitespaceRegex, ".*").r
    else
      fields.mkString(whitespaceRegex).r
  }

  private case class CloudfrontLogLine(
    date: String,
    time: String,
    lastIp: String,
    path: String,
    rfr: String,
    ua: String,
    qs: String,
    forwardedFor: String = "-"
  ) {
    def toValidatedMaybeCollectorPayload: ValidatedNel[FailureDetails.CPFormatViolationMessage, Option[CollectorPayload]] = {
      val timestamp = toTimestamp(date, time)
      val querystring =
        parseQuerystring(toOption(singleEncodePcts(qs)), CollectorEncoding)

      // No validation (yet) on the below
      val ip = IpAddressExtractor.extractIpAddress(forwardedFor, lastIp)
      val userAgent = singleEncodePcts(ua)
      val refr = singleEncodePcts(rfr)
      val referer = toOption(refr) map toCleanUri

      val collectorApi = CollectorPayload.parseApi(path)

      (timestamp.toValidatedNel, querystring.toValidatedNel, collectorApi.toValidatedNel).mapN { (t, q, a) =>
        val source = CollectorPayload.Source(CollectorName, CollectorEncoding.toString, None)
        val context =
          CollectorPayload.Context(Some(t), toOption(ip), toOption(userAgent), referer, Nil, None)
        CollectorPayload(a, q, None, None, source, context).some
      }
    }
  }
}
