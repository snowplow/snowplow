package com.snowplowanalytics.snowplow.enrich.badrows

import BadRow._

import cats.instances.list._
import cats.instances.either._
import cats.syntax.either._
import cats.syntax.alternative._

object EnrichPseudoCode {
  def parseRawPayload(line: String): Either[FormatViolation, RawPayload]                                      = ???
  def parseCollectorPayloads(rawPayload: RawPayload): List[Either[TrackerProtocolViolation, SnowplowPayload]] = ???
  def enrich(payload: SnowplowPayload): Either[BadRow, Event]                                                 = ???

  /**
   * Either fail on first step of parsing collector payload or
   * proceed and split all payloads into good events and bad (for specific reason row)
   */
  def process(line: String): Either[FormatViolation, (List[BadRow], List[Event])] =
    for {
      // At this step, line is just random string (array of bytes) we received from collector
      // rawPayload is at least valid JSON or GET, still it contain many events
      rawPayload <- parseRawPayload(line)
      // At this step, collectorPayloads are collector-agnostic, e.g. we don't know if
      // it was stream collector or clojure, payloads are just individual Maps
      // from now and then - all events fail only individually
      (trackerViolations, collectorPayloads) = parseCollectorPayloads(rawPayload).separate
      // Bad is either failed enrichment or schema-invalidation
      (bad, good) = collectorPayloads.map(enrich).separate
    } yield (trackerViolations ++ bad, good)
}
