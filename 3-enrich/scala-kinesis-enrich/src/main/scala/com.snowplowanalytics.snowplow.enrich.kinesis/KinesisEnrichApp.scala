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

package com.snowplowanalytics.snowplow.enrich.kinesis

// Config
import com.typesafe.config.{Config, ConfigFactory}

// Argot
import org.clapper.argot.ArgotParser

// Java
import java.io.{File,FileInputStream,IOException}
import java.net.InetAddress
import java.util.{Properties,UUID}

// Amazon.
import com.amazonaws.AmazonClientException
import com.amazonaws.auth.{
  AWSCredentials,
  BasicAWSCredentials,
  AWSCredentialsProvider,
  ClasspathPropertiesFileCredentialsProvider
}
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration,
  Worker
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory


class KinesisEnrichConfig(config: Config) {
  private val enrich = config.resolve.getConfig("enrich")

  private val aws = enrich.getConfig("aws")
  val accessKey = aws.getString("access-key")
  val secretKey = aws.getString("secret-key")

  private val streams = enrich.getConfig("streams")

  private val inStreams = streams.getConfig("in")
  val rawInStream = inStreams.getString("raw")

  private val outStreams = streams.getConfig("out")
  val enrichedOutStream = outStreams.getString("enriched")
  val enrichedOutStreamShards = outStreams.getInt("enriched_shards")
  val badOutStream = outStreams.getString("bad")
  val badOutStreamShards = outStreams.getInt("bad_shards")

  val appName = streams.getString("app-name")

  val initialPosition = streams.getString("initial-position")
  val streamEndpoint = streams.getString("endpoint")

  private val enrichments = enrich.getConfig("enrichments")
  private val geoIp = enrichments.getConfig("geo_ip")
  val geoIpEnabled = geoIp.getBoolean("enabled")
  val maxmindFile = new File(geoIp.getString("maxmind_file"))

  private val anonIp = enrichments.getConfig("anon_ip")
  val anonIpEnabled = anonIp.getBoolean("enabled")
  val anonOctets = anonIp.getInt("anon_octets")
}

object KinesisEnrichApp extends App {
  val parser = new ArgotParser(
    programName = generated.Settings.name,
    compactUsage = true,
    preUsage = Some("%s: Version %s. Copyright (c) 2013, %s.".format(
      generated.Settings.name,
      generated.Settings.version,
      generated.Settings.organization)
    )
  )

  // Optional config argument
  val config = parser.option[Config](
      List("config"), "filename", """
        |Configuration file. Defaults to \"resources/default.conf\"
        |(within .jar) if not set""".stripMargin) {
    (c, opt) =>
      val file = new File(c)
      if (file.exists) {
        ConfigFactory.parseFile(file)
      } else {
        parser.usage("Configuration file \"%s\" does not exist".format(c))
        ConfigFactory.empty()
      }
  }

  parser.parse(args)
  val kinesisEnrichConfig = new KinesisEnrichConfig(
    config.value.getOrElse(ConfigFactory.load("default"))
  )

  val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
    ":" + UUID.randomUUID()
  println("Using workerId: " + workerId)

  val kinesisProvider = createKinesisProvider(
    kinesisEnrichConfig.accessKey,
    kinesisEnrichConfig.secretKey
  )
  val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
    kinesisEnrichConfig.appName,
    kinesisEnrichConfig.rawInStream, 
    kinesisProvider,
    workerId
  ).withInitialPositionInStream(
    InitialPositionInStream.valueOf(kinesisEnrichConfig.initialPosition)
  )
  
  println(s"Running: ${kinesisEnrichConfig.appName}.")
  println(s"Processing raw input stream: ${kinesisEnrichConfig.rawInStream}")

  val kinesisEnrichedSink = new KinesisSink(kinesisProvider)
  val successful = kinesisEnrichedSink.createAndLoadStream(
    kinesisEnrichConfig.enrichedOutStream,
    kinesisEnrichConfig.enrichedOutStreamShards
  )
  if (!successful) {
    println("Error initializing or connecting to the stream.")
    sys.exit(-1)
  }

  
  val rawEventProcessorFactory = new RawEventProcessorFactory(
    kinesisEnrichConfig,
    kinesisEnrichedSink
  )
  val worker = new Worker(
    rawEventProcessorFactory,
    kinesisClientLibConfiguration,
    new NullMetricsFactory()
  )

  worker.run()

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
}

class BasicAWSCredentialsProvider(basic: BasicAWSCredentials) extends
    AWSCredentialsProvider{
  @Override def getCredentials: AWSCredentials = basic
  @Override def refresh = {}
}
