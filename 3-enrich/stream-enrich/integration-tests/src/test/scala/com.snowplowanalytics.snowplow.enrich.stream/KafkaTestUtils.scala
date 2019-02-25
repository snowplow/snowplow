/*
 * Copyright (c) 2013-2019 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.enrich.stream

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.ZkUtils
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import scala.util.Random

class KafkaTestUtils {
  // zk
  private val zkHost = "localhost"
  private val zkPort = 2181
  private val zkSessionTimeout = 6000
  private val zkConnectionTimeout = 6000
  private var zk: EmbeddedZookeeper = _
  private var zkUtils: ZkUtils = _
  private var zkReady = false

  // kafka
  private val brokerHost = "localhost"
  private val brokerPort = 9092
  private var kafkaServer: KafkaServerStartable = _
  private var topicCountMap = Map.empty[String, Int]
  private var brokerReady = false

  /** Zookeeper address */
  def zkAddress: String = {
    assert(zkReady, "Zk not ready, cannot get address")
    s"$zkHost:$zkPort"
  }

  /** Kafka broker address */
  def brokerAddress: String = {
    assert(brokerReady, "Broker not ready, cannot get address")
    s"$brokerHost:$brokerPort"
  }

  /** Zookeeper client */
  def zookeeperClient: ZkUtils = {
    assert(zkReady, "Zk not ready, cannot get zk client")
    Option(zkUtils).getOrElse(
      throw new IllegalStateException("Zk client not initialized"))
  }

  /** Start the Zookeeper and Kafka servers */
  def setup(): Unit = {
    setupEmbeddedZookeeper()
    setupEmbeddedKafkaServer()
  }

  private def setupEmbeddedZookeeper(): Unit = {
    zk = new EmbeddedZookeeper(zkHost, zkPort)
    zkUtils = ZkUtils(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout,
      isZkSecurityEnabled = false)
    zkReady = true
  }

  private def setupEmbeddedKafkaServer(): Unit = {
    assert(zkReady, "Zk should be setup beforehand")
    val kafkaConfig = new KafkaConfig(brokerProps)
    kafkaServer = new KafkaServerStartable(kafkaConfig)
    kafkaServer.startup()
    brokerReady = true
  }

  /** Close the Kafka as well as the Zookeeper client and server */
  def tearDown(): Unit = {
    brokerReady = false
    zkReady = false

    if (kafkaServer != null) {
      kafkaServer.shutdown()
      kafkaServer = null
    }

    if (zkUtils != null) {
      zkUtils.close()
      zkUtils = null
    }

    if (zk != null) {
      zk.shutdown()
      zk = null
    }

    topicCountMap = Map.empty
  }

  /** Create one or more topics */
  @scala.annotation.varargs
  def createTopics(topics: String*): Unit =
    for (topic <- topics) {
      AdminUtils.createTopic(zkUtils, topic, 1, 1)
      Thread.sleep(1000)
      topicCountMap = topicCountMap + (topic -> 1)
    }

  private def brokerProps: Properties = {
    val props = new Properties
    props.put("broker.id", "0")
    props.put("host.name", brokerHost)
    props.put("offsets.topic.replication.factor", "1")
    props.put("log.dir",
      {
        val dir = System.getProperty("java.io.tmpdir") +
          "/logDir-" + new Random().nextInt(Int.MaxValue)
        val f = new File(dir)
        f.mkdirs()
        dir
      }
    )
    props.put("port", brokerPort.toString)
    props.put("zookeeper.connect", zkAddress)
    props.put("zookeeper.connection.timeout.ms", "10000")
    props
  }

  private class EmbeddedZookeeper(hostname: String, port: Int) {
    private val snapshotDir = {
      val f = new File(System.getProperty("java.io.tmpdir"),
        "snapshotDir-" + Random.nextInt(Int.MaxValue))
      f.mkdirs()
      f
    }
    private val logDir = {
      val f = new File(System.getProperty("java.io.tmpdir"),
        "logDir-" + Random.nextInt(Int.MaxValue))
      f.mkdirs()
      f
    }

    private val factory = {
      val zkTickTime = 500
      val zk = new ZooKeeperServer(snapshotDir, logDir, zkTickTime)
      val f = new NIOServerCnxnFactory
      val maxCnxn = 16
      f.configure(new InetSocketAddress(hostname, port), maxCnxn)
      f.startup(zk)
      f
    }

    def shutdown(): Unit = {
      factory.shutdown()
      snapshotDir.delete()
      logDir.delete()
      ()
    }
  }
}
