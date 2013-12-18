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

package com.snowplowanalytics.scala_collector

import org.rogach.scallop.ScallopConf

class ScalaCollectorConf(args: Seq[String]) extends ScallopConf(args) {
  version(s"${generated.Settings.name}: " +
    s"Version ${generated.Settings.version} " +
    s"Copyright (c) 2013, ${generated.Settings.organization}.")
  // TODO banner("TODO")

  val interface = opt[String](default=Some("localhost"),
    descr="The interface to bind to.")
  val port = opt[Int](default=Some(8080),
    descr="The port to bind to.")
}
