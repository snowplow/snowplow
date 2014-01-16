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

// Snowplow
import sinks._
import com.snowplowanalytics.maxmind.geoip.IpGeo
import common.outputs.CanonicalOutput
import common.inputs.ThriftLoader
import common.MaybeCanonicalInput
import common.outputs.CanonicalOutput
import common.enrichments.EnrichmentManager
import common.enrichments.PrivacyEnrichments.AnonOctets

// Amazon
import com.amazonaws.auth._

abstract class AbstractSource(config: KinesisEnrichConfig) {
  def run(): Unit

  /**
   * Fields in our CanonicalOutput which are discarded for legacy
   * Redshift space reasons
   */
  private val DiscardedFields = Array("page_url", "page_referrer")

  protected val kinesisProvider = createKinesisProvider(
    config.accessKey,
    config.secretKey
  )

  protected val sink: ISink = SinkFactory.makeSink(config, kinesisProvider)

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

  def enrichEvent(binaryData: Array[Byte]): String = {
    val canonicalInput = ThriftLoader.toCanonicalInput(
      new String(binaryData.map(_.toChar))
    )

    (canonicalInput.toValidationNel) map { (ci: MaybeCanonicalInput) =>
      if (ci.isDefined) {
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
          ci.get
        )
        (canonicalOutput.toValidationNel) map { (co: CanonicalOutput) =>
          val ts = tabSeparateCanonicalOutput(co)
          if (config.sink != Sink.Test) {
            sink.storeCanonicalOutput(ts, co.user_ipaddress)
          } else {
            return ts
          }
          // TODO: Store bad event if canonical output not validated.
        }
      } else {
        // CanonicalInput is None: do nothing
      }
      // TODO: Store bad event if canonical input not validated.
    }
    return null
  }

  private def createKinesisProvider(accessKey: String, secretKey: String):
      AWSCredentialsProvider =
    if (isCpf(accessKey) && isCpf(secretKey)) {
        new ClasspathPropertiesFileCredentialsProvider()
    } else if (isCpf(accessKey) || isCpf(secretKey)) {
      throw new RuntimeException(
        "access-key and secret-key must both be set to 'cpf', or neither"
      )
    } else {
      new BasicAWSCredentialsProvider(
        new BasicAWSCredentials(accessKey, secretKey)
      )
    }
  private def isCpf(key: String): Boolean = (key == "cpf")

  class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
      AWSCredentialsProvider{
    @Override def getCredentials: AWSCredentials = basic
    @Override def refresh = {}
  }
}
