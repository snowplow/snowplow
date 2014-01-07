 /*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. with significant
 * portions copyright 2012-2014 Amazon.
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

package com.snowplowanalytics.kinesis.consumer

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


class KinesisConsumerConfig(config: Config) {
  private val consumer = config.getConfig("consumer")

  private val aws = consumer.getConfig("aws")
  val accessKey = aws.getString("access-key")
  val secretKey = aws.getString("secret-key")

  private val stream = consumer.getConfig("stream")
  val appName = stream.getString("app-name")
  val streamName = stream.getString("stream-name")
  val initialPosition = stream.getString("initial-position")
  val streamDataType = stream.getString("data-type")
  val streamEndpoint = stream.getString("endpoint")
}

object KinesisConsumerApp extends App {
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
  val kinesisConsumerConfig = new KinesisConsumerConfig(
    config.value.getOrElse(ConfigFactory.load("default"))
  )

  val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
    ":" + UUID.randomUUID()
  println("Using workerId: " + workerId)

  val kinesisCredentials = createKinesisCredentials(
    kinesisConsumerConfig.accessKey,
    kinesisConsumerConfig.secretKey
  )
  val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
    kinesisConsumerConfig.appName,
    kinesisConsumerConfig.streamName, 
    kinesisCredentials,
    workerId
  ).withInitialPositionInStream(
    InitialPositionInStream.valueOf(kinesisConsumerConfig.initialPosition)
  )
  
  println(s"Running: ${kinesisConsumerConfig.appName}.")
  println(s"Processing stream: ${kinesisConsumerConfig.streamName}")
  
  val recordProcessorFactory = new RecordProcessorFactory(kinesisConsumerConfig)
  val worker = new Worker(
    recordProcessorFactory,
    kinesisClientLibConfiguration,
    new NullMetricsFactory()
  )

  worker.run()

  private def createKinesisCredentials(accessKey: String, secretKey: String):
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
