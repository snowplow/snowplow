package com.snowplowanalytics.snowplow.enrich

import com.twitter.scalding._
import com.twitter.scalding.commons.source._
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

case class LzoThriftSource(p: String) extends FixedPathLzoThrift[SnowplowRawEvent](p: String)

