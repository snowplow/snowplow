/* 
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0, and
 * you may not use this file except in compliance with the Apache License
 * Version 2.0.  You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Apache License Version 2.0 is distributed on an "AS
 * IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */

package com.snowplowanalytics.snowplow.collectors.thrift

import scala.collection.JavaConversions._

// Specs2 and ScalaCheck
import org.specs2.mutable.Specification
import org.scalacheck.{Arbitrary,Gen,Properties}
import org.scalacheck.Prop.forAll

object SnowplowEventSpec extends Properties("SnowplowEvent"){
  property("timestamp") = forAll { (timestamp: Long) =>
    val event = new SnowplowEvent(timestamp, null, "collector", "encoding")
    event.getTimestamp == timestamp
  }
  // TODO: PayloadProtocol.Http
  // TODO: PayloadFormat.{HttpGet,HttpPostUrlencodedForm,HttpPostMultipartForm}
  property("payloadData") = forAll { (payloadData: String) =>
    val payload = new TrackerPayload(
      PayloadProtocol.Http, PayloadFormat.HttpGet, payloadData)
    val event = new SnowplowEvent(0L, payload, "collector", "encoding")
    event.getPayload.getData == payloadData
  }
  property("collector") = forAll { (collector: String) =>
    val event = new SnowplowEvent(0L, null, collector, "encoding")
    event.getCollector == collector
  }
  property("encoding") = forAll { (encoding: String) =>
    val event = new SnowplowEvent(0L, null, "collector", encoding)
    event.getEncoding == encoding
  }

  // Check optional variables.
  type setFunc = Function2[SnowplowEvent,String,SnowplowEvent]
  type getFunc = Function1[SnowplowEvent,String]
  val f_hostname_set: setFunc = _.setHostname(_)
  val f_hostname_get: getFunc = _.getHostname()
  val f_ipAddress_set: setFunc = _.setIpAddress(_)
  val f_ipAddress_get: getFunc = _.getIpAddress()
  val f_userAgent_set: setFunc = _.setUserAgent(_)
  val f_userAgent_get: getFunc = _.getUserAgent()
  val f_refererUri_set: setFunc = _.setRefererUri(_)
  val f_refererUri_get: getFunc = _.getRefererUri()
  val f_userId_set: setFunc = _.setUserId(_)
  val f_userId_get: getFunc = _.getUserId()
  for (optionalVar <- List(
        ("hostname", f_hostname_set, f_hostname_get),
        ("ipAddress", f_ipAddress_set, f_ipAddress_get),
        ("userAgent", f_userAgent_set, f_userAgent_get),
        ("refererUri", f_refererUri_set, f_refererUri_get),
        ("userId", f_userId_set, f_userId_get)
      )) {
    property(optionalVar._1) = forAll { (value: String) =>
      val event = new SnowplowEvent(0L, null, "collector", "encoding")
      optionalVar._2(event, value)
      optionalVar._3(event) == value
    }
  }

  property("headers") = forAll { (headers: List[String]) =>
    val event = new SnowplowEvent(0L, null, "collector", "encoding")
    event.setHeaders(headers)
    event.getHeaders.toList.equals(headers)
  }
}
