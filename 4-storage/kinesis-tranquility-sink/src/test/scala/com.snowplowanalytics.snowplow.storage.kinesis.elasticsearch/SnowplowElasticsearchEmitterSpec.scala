/*
 * Copyright (c) 2014 Snowplow Analytics Ltd. All rights reserved.
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

// Specs2
import org.specs2.mutable.Specification
import org.specs2.scalaz.ValidationMatchers

// This project
import sinks._

/**
 * Tests Shredder
 */
class SnowplowElasticsearchEmitterSpec extends Specification with ValidationMatchers {

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
  }

}
