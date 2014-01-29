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

abstract class AbstractSource(config: KinesisEnrichConfig) {
  def run

  /**
   * Fields in our CanonicalOutput which are discarded for legacy
   * Redshift space reasons
   */
  private val DiscardedFields = Array("page_url", "page_referrer")

  // Initialize a kinesis provider to use with a Kinesis source or sink.
  protected val kinesisProvider = createKinesisProvider

  // Initialize the sink to output enriched events to.
  protected val sink: ISink = config.sink match {
    case Sink.Kinesis => new KinesisSink(kinesisProvider, config)
    case Sink.Stdouterr => new StdouterrSink
    case Sink.Test => null
  }

  // Iterate through an enriched CanonicalOutput object and tab separate
  // the fields to a string.
  def tabSeparateCanonicalOutput(output: CanonicalOutput): String = {
    output.getClass.getDeclaredFields
    .filter { field =>
      !DiscardedFields.contains(field.getName)
    }
    .map{ field =>
      field.setAccessible(true)
      Option(field.get(output)).getOrElse("")
    }.mkString("\t")
  }

  // Helper method to enrich an event.
  def enrichEvent(binaryData: Array[Byte]): String = {
    val canonicalInput = ThriftLoader.toCanonicalInput(
      new String(binaryData.map(_.toChar))
    )

    canonicalInput.toValidationNel match { 

      case Failure(f) =>
        println("// TODO: Store bad event if canonical input not validated: " + f)
      case Success(None) =>
        println("// CanonicalInput is None: do nothing")
      case Success(Some(ci)) => {
        val ipGeo = new IpGeo(
          dbFile = config.maxmindFile,
          memCache = false,
          lruCache = 20000
        )
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
            val ts = tabSeparateCanonicalOutput(co)
            if (config.sink != Sink.Test) {
              sink.storeCanonicalOutput(ts, co.user_ipaddress)
            } else {
              return ts
            }
          case Failure(f) =>
            println("// TODO: Store bad event if canonical output not validated: " + f)
        }
      }
    }
    return null // TODO: change this method to Option[None]
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
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(a, s)
      )
    }
  }
  private def isCpf(key: String): Boolean = (key == "cpf")

  // Wrap BasicAWSCredential objects.
  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
