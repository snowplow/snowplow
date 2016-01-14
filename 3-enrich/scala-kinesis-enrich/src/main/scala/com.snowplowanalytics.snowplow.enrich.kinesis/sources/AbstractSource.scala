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
package com.snowplowanalytics
package snowplow.enrich
package kinesis
package sources

// Amazon
import com.amazonaws.auth._

// Apache commons
import org.apache.commons.codec.binary.Base64

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// json4s
import org.json4s.scalaz.JsonScalaz._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

// Iglu
import iglu.client.Resolver
import iglu.client.validation.ProcessingMessageMethods._

// Snowplow
import sinks._
import common.outputs.{
  EnrichedEvent,
  BadRow
}
import common.loaders.ThriftLoader
import common.enrichments.EnrichmentRegistry
import common.enrichments.EnrichmentManager
import common.adapters.AdapterRegistry

import common.ValidatedMaybeCollectorPayload
import common.EtlPipeline
import common.utils.JsonUtils

/**
 * Abstract base for the different sources
 * we support.
 */
abstract class AbstractSource(config: KinesisEnrichConfig, igluResolver: Resolver,
                              enrichmentRegistry: EnrichmentRegistry) {
  
  /**
   * Never-ending processing loop over source stream.
   */
  def run

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = createKinesisProvider

  // Initialize the sink to output enriched events to.
  protected val sink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config, InputType.Good).some
    case Sink.Stdouterr => new StdouterrSink(InputType.Good).some
    case Sink.Test => None
  }

  protected val badSink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config, InputType.Bad).some
    case Sink.Stdouterr => new StdouterrSink(InputType.Bad).some
    case Sink.Test => None
  }

  implicit val resolver: Resolver = igluResolver

  // Iterate through an enriched EnrichedEvent object and tab separate
  // the fields to a string.
  def tabSeparateEnrichedEvent(output: EnrichedEvent): String = {
    output.getClass.getDeclaredFields
    .map{ field =>
      field.setAccessible(true)
      Option(field.get(output)).getOrElse("")
    }.mkString("\t")
  }

  /**
   * Convert incoming binary Thrift records to lists of enriched events
   *
   * @param binaryData Thrift raw event
   * @return List containing successful or failed events, each with a
   *         partition key
   */
  def enrichEvents(binaryData: Array[Byte]): List[Validation[(String, String), (String, String)]] = {
    val canonicalInput: ValidatedMaybeCollectorPayload = ThriftLoader.toCollectorPayload(binaryData)
    val processedEvents: List[ValidationNel[String, EnrichedEvent]] = EtlPipeline.processEvents(
      enrichmentRegistry, s"kinesis-${generated.Settings.version}", System.currentTimeMillis.toString, canonicalInput)
    processedEvents.map(validatedMaybeEvent => {
      validatedMaybeEvent match {
        case Success(co) => (tabSeparateEnrichedEvent(co) -> co.user_ipaddress).success
        case Failure(errors) => {
          val line = new String(Base64.encodeBase64(binaryData))
          (BadRow(line, errors).toCompactJson -> "fail").fail
        }
      }
    })
  }

  /**
   * Deserialize and enrich incoming Thrift records and store the results
   * in the appropriate sinks. If doing so causes the number of events
   * stored in a sink to become sufficiently large, all sinks are flushed
   * and we return `true`, signalling that it is time to checkpoint
   *
   * @param binaryData Thrift raw event
   * @return Whether to checkpoint
   */
  def enrichAndStoreEvents(binaryData: List[Array[Byte]]): Boolean = {
    val enrichedEvents = binaryData.flatMap(enrichEvents(_))
    val successes = enrichedEvents collect { case Success(s) => s }
    val failures = enrichedEvents collect { case Failure(s) => s }
    val successesTriggeredFlush = sink.map(_.storeEnrichedEvents(successes))
    val failuresTriggeredFlush = badSink.map(_.storeEnrichedEvents(failures))
    if (successesTriggeredFlush == Some(true) || failuresTriggeredFlush == Some(true)) {

      // Block until the records have been sent to Kinesis
      sink.foreach(_.flush)
      badSink.foreach(_.flush)
      true
    } else {
      false
    }

  }


  // Initialize a Kinesis provider with the given credentials.
  private def createKinesisProvider(): AWSCredentialsProvider =  {
    val a = config.accessKey
    val s = config.secretKey
    if (isCpf(a) && isCpf(s)) {
        new ClasspathPropertiesFileCredentialsProvider()
    } else if (isCpf(a) || isCpf(s)) {
      throw new RuntimeException(
        "access-key and secret-key must both be set to 'cpf', or neither"
      )
    } else if (isIam(a) && isIam(s)) {
      new InstanceProfileCredentialsProvider()
    } else if (isIam(a) || isIam(s)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'iam', or neither")
    } else if (isEnv(a) && isEnv(s)) {
      new EnvironmentVariableCredentialsProvider()
    } else if (isEnv(a) || isEnv(s)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'env', or neither")
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(a, s)
      )
    }
  }

  /**
   * Is the access/secret key set to the special value "cpf" i.e. use
   * the classpath properties file for credentials.
   *
   * @param key The key to check
   * @return true if key is cpf, false otherwise
   */
  private def isCpf(key: String): Boolean = (key == "cpf")

  /**
   * Is the access/secret key set to the special value "iam" i.e. use
   * the IAM role to get credentials.
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isIam(key: String): Boolean = (key == "iam")

  /**
   * Is the access/secret key set to the special value "env" i.e. get
   * the credentials from environment variables
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isEnv(key: String): Boolean = (key == "env")

  // Wrap BasicAWSCredential objects.
  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
