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
package adapters
package registry

// Joda-Time
import org.joda.time.DateTime

// Scalaz
import scalaz._
import Scalaz._

// Snowplow
import loaders.{
  CollectorApi,
  CollectorSource,
  CollectorContext,
  CollectorPayload
}
import SpecHelpers._

// Specs2
import org.specs2.{Specification, ScalaCheck}
import org.specs2.matcher.DataTables
import org.specs2.scalaz.ValidationMatchers

class SnowplowAdapterSpec extends Specification with DataTables with ValidationMatchers with ScalaCheck { def is =

  "This is a specification to test the SnowplowAdapter functionality"                                                   ^
                                                                                                                       p^
  "Tp1.toRawEvents should return a NEL containing one RawEvent if the querystring is populated"                         ! e2^
  "Tp1.toRawEvents should return a Validation Failure if the querystring is empty"                                      ! e2^
                                                                                                                        end

  object Expected {
    val api: (String) => CollectorApi = version => CollectorApi("com.snowplowanalytics.snowplow", version)
  	val source = CollectorSource("clj-tomcat", "UTF-8", None)
  	val context = CollectorContext(DateTime.parse("2013-08-29T00:18:48.000+00:00"), "37.157.33.123".some, None, None, Nil, None)
  }

  implicit val resolver = SpecHelpers.IgluResolver

  def e1 = {
    val api = Expected.api("tp1")
    val payload = CollectorPayload(api, toNameValuePairs("aid" -> "test"), None, None, Expected.source, Expected.context)
    val actual = SnowplowAdapter.Tp1.toRawEvents(payload)
    actual must beSuccessful(NonEmptyList(RawEvent(api, Map("aid" -> "test"), None, Expected.source, Expected.context)))
  }

  def e2 = {
  	val payload = CollectorPayload(Expected.api("tp1"), Nil, None, None, Expected.source, Expected.context)
  	val actual = SnowplowAdapter.Tp1.toRawEvents(payload)
  	actual must beFailing(NonEmptyList("Querystring is empty: no raw event to process"))
  }
}