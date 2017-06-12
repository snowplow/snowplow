/**
 * Copyright (c) 2014-2016 Snowplow Analytics Ltd. All rights reserved.
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

package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Java
import java.util.Properties

import com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch.clients.{ElasticsearchSender, IElasticsearchSender}

import scala.collection.mutable.ListBuffer

// AWS libs
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  UnmodifiableBuffer
}
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer

// Scala
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

// Scalaz
import scalaz._
import Scalaz._

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

// Logging
import org.apache.commons.logging.{
  LogFactory
}

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

// This project
import sinks._

/**
 * Tests Shredder
 */
class SnowplowElasticsearchEmitterSpec extends Specification with ValidationMatchers {

  private val Log = LogFactory.getLog(getClass)

  class MockElasticsearchSender extends ElasticsearchSender {
    var sentRecords: List[(String, Validation[List[String], ElasticsearchObject])] = List()
    var callCount: Int = 0
    val calls: ListBuffer[List[(String, Validation[List[String], ElasticsearchObject])]] = new ListBuffer

    override def sendToElasticsearch(records: List[(String, Validation[List[String], ElasticsearchObject])]) = {
      sentRecords = sentRecords ::: records
      callCount += 1
      calls += records
      List()
    }

    override def close() = {}

  }

  "The emit method" should {
    "return all invalid records" in {

      val kcc = new KinesisConnectorConfiguration(new Properties, new DefaultAWSCredentialsProviderChain)
      val eem = new SnowplowElasticsearchEmitter(kcc, Some(new StdouterrSink), new StdouterrSink)

      val validInput: EmitterInput = "good" -> new ElasticsearchObject("index", "type", "{}").success
      val invalidInput: EmitterInput = "bad" -> List("malformed event").fail

      val input = List(validInput, invalidInput)

      val bmb = new BasicMemoryBuffer[EmitterInput](kcc, input)
      val ub = new UnmodifiableBuffer[EmitterInput](bmb)

      val actual = eem.emit(ub)

      actual must_== List(invalidInput).asJava
    }

    "send multiple records in seperate requests where single record size > buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1000")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)


      val ess = new MockElasticsearchSender
      val eem = new SnowplowElasticsearchEmitter(kcc, None, new StdouterrSink, None, 60000, Some(ess))

      val validInput: EmitterInput = "good" -> new ElasticsearchObject("index" * 10000, "type", "{}").success

      val input = List.fill(50)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterInput](kcc, input)
      val ub = new UnmodifiableBuffer[EmitterInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 50
      forall (ess.calls) { c => c.length mustEqual 1 }
    }

    "send a single record in 1 request where record size > buffer bytes size " in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1000")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)


      val ess = new MockElasticsearchSender
      val eem = new SnowplowElasticsearchEmitter(kcc, None, new StdouterrSink, None, 60000, Some(ess))

      val validInput: EmitterInput = "good" -> new ElasticsearchObject("index" * 10000, "type", "{}").success

      val input = List(validInput)

      val bmb = new BasicMemoryBuffer[EmitterInput](kcc, input)
      val ub = new UnmodifiableBuffer[EmitterInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall (ess.calls) { c => c.length mustEqual 1 }
    }

    "send multiple records in 1 request where total byte size < buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1048576")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)

      val ess = new MockElasticsearchSender
      val eem = new SnowplowElasticsearchEmitter(kcc, None, new StdouterrSink, None, 60000, Some(ess))

      val validInput: EmitterInput = "good" -> new ElasticsearchObject("index", "type", "{}").success

      val input = List.fill(50)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterInput](kcc, input)
      val ub = new UnmodifiableBuffer[EmitterInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall (ess.calls) { c => c.length mustEqual 50 }
    }

    "send a single record in 1 request where single record size < buffer bytes size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "1048576")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)


      val ess = new MockElasticsearchSender
      val eem = new SnowplowElasticsearchEmitter(kcc, None, new StdouterrSink, None, 60000, Some(ess))

      val validInput: EmitterInput = "good" -> new ElasticsearchObject("index", "type", "{}").success

      val input = List(validInput)

      val bmb = new BasicMemoryBuffer[EmitterInput](kcc, input)
      val ub = new UnmodifiableBuffer[EmitterInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 1
      forall (ess.calls) { c => c.length mustEqual 1 }
    }

    "send multiple records in batches where single record byte size < buffer size and total byte size > buffer size" in {
      val props = new Properties
      props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, "200")

      val kcc = new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain)


      val ess = new MockElasticsearchSender
      val eem = new SnowplowElasticsearchEmitter(kcc, None, new StdouterrSink, None, 60000, Some(ess))

      // record size is 95 bytes
      val validInput: EmitterInput = "good" -> new ElasticsearchObject("index", "type", "{}").success

      val input = List.fill(20)(validInput)

      val bmb = new BasicMemoryBuffer[EmitterInput](kcc, input)
      val ub = new UnmodifiableBuffer[EmitterInput](bmb)

      eem.emit(ub)

      ess.sentRecords mustEqual input
      ess.callCount mustEqual 10 // 10 buffers of 2 records each
      forall (ess.calls) { c => c.length mustEqual 2 }
    }
  }

}
