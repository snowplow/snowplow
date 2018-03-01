package com.snowplowanalytics.snowplow.enrich

import scala.util.matching.Regex

package object stream {
  type StringOrRegex = Either[String, Regex]
  val JustString = Left
  val JustRegex  = Right
}
