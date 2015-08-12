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
package enrichments

// Java
import java.util.UUID

// Scala
import scala.util.control.NonFatal

// Scalaz
import scalaz._
import Scalaz._

// Joda-Time
import org.joda.time.{DateTime, DateTimeZone, Period}
import org.joda.time.format.DateTimeFormat

// This project
import utils.{ConversionUtils => CU}
import utils.{JsonUtils => JU}

/**
 * Holds the enrichments related to events.
 */
object EventEnrichments {

  /**
   * A Redshift-compatible timestamp format
   */
  private val TstampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(DateTimeZone.UTC)

  /**
   * Converts a Joda DateTime into
   * a Redshift-compatible timestamp String.
   *
   * @param datetime The Joda DateTime
   *        to convert to a timestamp String
   * @return the timestamp String
   */
  def toTimestamp(datetime: DateTime): String = TstampFormat.print(datetime)

   /**
    * Converts a Redshift-compatible timestamp String
    * back into a Joda DateTime.
    *
    * @param timestamp The timestamp String to convert
    * @return the Joda DateTime
    */
  def fromTimestamp(timestamp: String): DateTime = TstampFormat.parseDateTime(timestamp)

  /**
   * Make a collector_tstamp Redshift-compatible
   *
   * @param Optional collectorTstamp
   * @return Validation boxing the result of making the timestamp Redshift-compatible
   */
  def formatCollectorTstamp(collectorTstamp: Option[DateTime]): Validation[String, String] = {
    collectorTstamp match {
      case None => "No collector_tstamp set".fail
      case Some(tstamp) => {
        val formattedTimestamp = toTimestamp(tstamp)
        if (formattedTimestamp.startsWith("-")) {
          s"Collector timestamp $formattedTimestamp is negative and will fail the Redshift load".fail
        } else {
          formattedTimestamp.success
        }
      }
    }
  }

  /**
   * Validate that the collector timestamp is set and valid
   *
   * @param collectorTstamp
   * @return Validated collector timestamp
   */
  def validateCollectorTstamp(collectorTstamp: Option[DateTime]): Validation[String, String] = {
    collectorTstamp match {
      case None => "No collector_tstamp set".fail
      case Some(t) => extractTimestamp("collector_tstamp", t.getMillis.toString)
    }
  }

  /**
   * Calculate the derived timestamp
   *
   * If dvce_sent_tstamp and dvce_created_tstamp are not null and the former is after the latter,
   * add the difference between the two to the collector_tstamp.
   * Otherwise just return the collector_tstamp.
   *
   * @aram dvceSentTstamp
   * @param dvceCreatedTstamp
   * @param collectorTstamp
   * @return derived timestamp
   */
  def getDerivedTimestamp(
    dvceSentTstamp: Option[String],
    dvceCreatedTstamp: Option[String],
    collectorTstamp: Option[String]): Validation[String, Option[String]] = try {
      ((dvceSentTstamp, dvceCreatedTstamp, collectorTstamp) match {
        case (Some(dst), Some(dct), Some(ct)) => {
          val startTstamp = fromTimestamp(dct)
          val endTstamp = fromTimestamp(dst)
          if (startTstamp.isBefore(endTstamp)) {
            toTimestamp(fromTimestamp(ct).minus(new Period(startTstamp, endTstamp))).some
          } else {
            ct.some
          }
        }
        case _ => collectorTstamp
      }).success
    } catch {
      case NonFatal(e) => s"Exception calculating derived timestamp: [$e]".fail
    }

  /**
   * Extracts the timestamp from the
   * format as laid out in the Tracker
   * Protocol:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-common-params
   *
   * @param tstamp The timestamp as
   *        stored in the Tracker
   *        Protocol
   * @return a Tuple of two Strings
   *         (date and time), or an
   *         error message if the
   *         format was invalid
   */
  val extractTimestamp: (String, String) => ValidatedString = (field, tstamp) =>
    try {
      val dt = new DateTime(tstamp.toLong)
      val timestampString = toTimestamp(dt)
      if (timestampString.startsWith("-")) {
        s"Field [$field]: [$tstamp] is formatted as [$timestampString] which isn't Redshift-compatible".fail
      } else {
        timestampString.success
      }
    } catch {
      case nfe: NumberFormatException =>
        "Field [%s]: [%s] is not in the expected format (ms since epoch)".format(field, tstamp).fail
    }

  /**
   * Turns an event code into a valid event type,
   * e.g. "pv" -> "page_view". See the Tracker
   * Protocol for details:
   *
   * https://github.com/snowplow/snowplow/wiki/snowplow-tracker-protocol#wiki-event2
   *
   * @param eventCode The event code
   * @return the event type, or an error message
   *         if not recognised, boxed in a Scalaz
   *         Validation
   */
  val extractEventType: (String, String) => ValidatedString = (field, code) =>
    code match {
      case "se" => "struct".success
      case "ev" => "struct".success        // Leave in for legacy.
      case "ue" => "unstruct".success
      case "ad" => "ad_impression".success // Leave in for legacy.
      case "tr" => "transaction".success
      case "ti" => "transaction_item".success
      case "pv" => "page_view".success
      case "pp" => "page_ping".success
      case  ec  => "Field [%s]: [%s] is not a recognised event code".format(field, ec).fail
    }

  /**
   * Returns a unique event ID. The event ID is 
   * generated as a type 4 UUID, then converted
   * to a String.
   *
   * () on the function signature because it's
   * not pure
   *
   * @return the unique event ID
   */
  def generateEventId(): String = UUID.randomUUID().toString
}