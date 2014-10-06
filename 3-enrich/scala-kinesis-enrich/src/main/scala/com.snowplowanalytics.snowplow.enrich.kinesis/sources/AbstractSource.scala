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
package com.snowplowanalytics.snowplow.enrich
package kinesis
package sources

// Amazon
import com.amazonaws.auth._

// Scalaz
import scalaz.{Sink => _, _}
import Scalaz._

// Snowplow
import sinks._
import com.snowplowanalytics.maxmind.geoip.IpGeo
import common.outputs.CanonicalOutput
import common.inputs.ThriftLoader
import common.MaybeCanonicalInput
import common.outputs.CanonicalOutput
import common.enrichments.EnrichmentManager
import common.enrichments.PrivacyEnrichments.AnonOctets

/**
 * Abstract base for the different sources
 * we support.
 */
abstract class AbstractSource(config: KinesisEnrichConfig) {

  /**
   * Never-ending processing loop over source stream.
   */
  def run

  /**
   * shutdown gracefully if necessary
   */
  def stop {
  }

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = createKinesisProvider

  // Initialize the sink to output enriched events to.
  protected val sink: Option[ISink] = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config).some
    case Sink.Kafka => new KafkaSink(config).some
    case Sink.Stdouterr => new StdouterrSink().some
    case Sink.Test => None
  }

  private lazy val ipGeo = new IpGeo(
    dbFile = config.maxmindFile,
    memCache = false,
    lruCache = 20000
  )

  // Iterate through an enriched CanonicalOutput object and tab separate
  // the fields to a string.
  def tabSeparateCanonicalOutput(output: CanonicalOutput): String = {
    output.getClass.getDeclaredFields
    .map{ field =>
      field.setAccessible(true)
      Option(field.get(output)).getOrElse("")
    }.mkString("\t")
  }

  // Helper method to enrich an event.
  // TODO: this is a slightly odd design: it's a pure function if our
  // our sink is Test, but it's an impure function (with
  // storeCanonicalOutput side effect) for the other sinks. We should
  // break this into a pure function with an impure wrapper.
  def enrichEvent(binaryData: Array[Byte]): Option[String] = {
    val canonicalInput = ThriftLoader.toCanonicalInput(binaryData)

    canonicalInput.toValidationNel match {

      case Failure(f)        => None
        // TODO: https://github.com/snowplow/snowplow/issues/463
      case Success(None)     => None // Do nothing
      case Success(Some(ci)) => {
        val anonOctets =
          if (!config.anonIpEnabled || config.anonOctets == 0) {
            AnonOctets.None
          } else {
            AnonOctets(config.anonOctets)
          }
        val canonicalOutput = EnrichmentManager.enrichEvent(
          ipGeo,
          s"kinesis-${generated.Settings.version}",
          anonOctets,
          ci
        )

        canonicalOutput.toValidationNel match {
          case Success(co) =>
            // ugly patch ... should be removed with scala-common-enrich 0.5.0
            if (co.network_userid == null) {
              ci.userId match {
              case s:Some[String] => co.network_userid = s.get
              case _ => ()
              }
            }

            val ts = tabSeparateCanonicalOutput(co)
            for (s <- sink) {
              // TODO: pull this side effect into parent function
              s.storeCanonicalOutput(ts, co.user_ipaddress)
            }
            Some(ts)
          case Failure(f)  => None
            // TODO: https://github.com/snowplow/snowplow/issues/463
        }
      }
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
      throw new RuntimeException("access-key and secret-key must both be set to 'iam', or neither of them")
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(a, s)
      )
    }
  }

  /**
   * Is the access/secret key set to the special value "iam" i.e. use
   * the IAM role to get credentials.
   *
   * @param key The key to check
   * @return true if key is iam, false otherwise
   */
  private def isIam(key: String): Boolean = (key == "iam")

  private def isCpf(key: String): Boolean = (key == "cpf")

  // Wrap BasicAWSCredential objects.
  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
