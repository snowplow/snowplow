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
package enrich
package kinesis
package sources

// Java
import java.io.{FileInputStream,IOException}
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.{List,UUID,Properties}

// Kafka
import kafka.common._
import kafka.utils._
import kafka.serializer._
//import kafka.message._
import kafka.consumer._

// Scala
//import scala.util.control.Breaks._
//import scala.collection.JavaConversions._

// Thrift
import org.apache.thrift.TDeserializer

// Snowplow events and enrichment
import sinks._
import collectors.thrift.{
  SnowplowRawEvent,
  TrackerPayload => ThriftTrackerPayload,
  PayloadProtocol,
  PayloadFormat
}

/**
 * Source to read events from a Kafka stream
 *
 * TODO: replace printlns with using Java logger
 */
class KafkaSource(config: KinesisEnrichConfig)
    extends AbstractSource(config) with Logging {
  
  var consumer = createKafkaConsumer(config: KinesisEnrichConfig)
  val filterSpec = new Whitelist(config.kafkaTopic)

  /**
   * Never-ending processing loop over source stream.
   */
  def run {
    val stream = consumer.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())
    stream.foreach {
      streams =>
        streams.foreach { message =>
          enrichEvent(message.message)
        }
    }
  }

  def createKafkaConsumer(config: KinesisEnrichConfig): ConsumerConnector = {
    //val clientId: String = InetAddress.getLocalHost.getHostName()
    val groupId = "kafka-enrich"
    info(s"Create Kafka Consumer '${groupId}' to zookeeper: ${config.zookeeper}")

    val props = new Properties()
    props.put("group.id", groupId)
    props.put("zookeeper.connect", config.zookeeper)
    props.put("auto.offset.reset", "smallest")
    
    val consconf = new ConsumerConfig(props)
    val consumer = Consumer.create(consconf)
    info("consumer created.")
    return consumer
  }

  override def stop {
    info("Shutting down!")
    consumer.shutdown()
  }
}
