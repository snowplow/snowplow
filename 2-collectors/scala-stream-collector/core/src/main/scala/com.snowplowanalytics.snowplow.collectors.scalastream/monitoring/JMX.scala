/*
 * Copyright (c) 2013-2020 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow.collectors.scalastream
package monitoring

import java.lang.management.ManagementFactory
import javax.management.{MBeanServer, ObjectName}
import scala.beans.BeanProperty

trait SnowplowMXBean {
  def getLatest(): Long
  def getRequests(): Int
  def getSuccessfulRequests(): Int
  def getFailedRequests(): Int

  def incrementSuccessfulRequests(): Unit
  def incrementFailedRequests(): Unit
}

class SnowplowScalaCollectorMetrics extends SnowplowMXBean {
  @BeanProperty
  var latest = now()

  @BeanProperty
  var requests = 0

  @BeanProperty
  var successfulRequests = 0

  @BeanProperty
  var failedRequests = 0

  override def incrementSuccessfulRequests(): Unit = {
    successfulRequests += 1
    incrementRequests()
  }

  override def incrementFailedRequests(): Unit = {
    failedRequests += 1
    incrementRequests()
  }

  private def incrementRequests(): Unit = {
    requests += 1
    latest = now()
  }

  def now(): Long = System.currentTimeMillis() / 1000L
}

object BeanRegistry {
  val collectorBean = new SnowplowScalaCollectorMetrics

  val mbs: MBeanServer = ManagementFactory.getPlatformMBeanServer
  val mBeanName: ObjectName =
    new ObjectName("com.snowplowanalytics.snowplow:type=ScalaCollector")

  mbs.registerMBean(collectorBean, mBeanName)
}
