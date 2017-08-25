 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache
 * License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the Apache License Version 2.0 for the specific language
 * governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow
package collectors
package scalastream

import model.Stdout
import sinks.Sink

// Allow the testing framework to test collection events using the
// same methods from AbstractSink as the other sinks.
class TestSink extends Sink {

  // Effectively no limit to the record size
  val MaxBytes = Long.MaxValue

  def storeRawEvents(events: List[Array[Byte]], key: String) = events

  override def getType = Stdout
}
