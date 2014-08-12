package com.snowplowanalytics.snowplow.sinks

// Java
import java.io.File
import java.util.Properties

// Argot
import org.clapper.argot._

// Config
import com.typesafe.config.{Config, ConfigFactory}

// AWS libs
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration

/**
 * The entrypoint class for the Kinesis-S3 Sink applciation.
 */
object SinkApp extends App {

  // Argument specifications
  import ArgotConverters._

  // General bumf for our app
  val parser = new ArgotParser(
    programName = "generated",
    compactUsage = true,
    preUsage = Some("Meow")
  )

  // Optional config argument
  val config = parser.option[Config](List("config"),
                                     "filename",
                                     "Configuration file. Defaults to \"resources/default.conf\" (within .jar) if not set") {
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

  val conf = config.value.getOrElse(ConfigFactory.load("default")) // Fall back to the /resources/default.conf

  val executor = new S3SinkExecutor(convertConfig(conf))
  executor.run()

  /**
   * This function converts the config file into the format
   * expected by the Kinesis connector interfaces.
   */
  def convertConfig(conf: Config): KinesisConnectorConfiguration = {
    val props = new Properties()
    val connector = conf.resolve.getConfig("connector")

    val aws = connector.getConfig("aws")
    val accessKey = aws.getString("access-key")
    val secretKey = aws.getString("secret-key")

    val kinesis = connector.getConfig("kinesis")
    val kEndpoint = kinesis.getString("endpoint")
    val streamName = kinesis.getString("stream-name")
    val appName = kinesis.getString("app-name")

    val s3 = connector.getConfig("s3")
    val s3Endpoint = s3.getString("endpoint")
    val bucket = s3.getString("bucket")

    val buffer = connector.getConfig("buffer")
    val byteLimit = buffer.getString("byte-limit")
    val recordLimit = buffer.getString("record-limit")

    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_INPUT_STREAM, streamName)
    props.setProperty(KinesisConnectorConfiguration.PROP_KINESIS_ENDPOINT, kEndpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_APP_NAME, appName)

    props.setProperty(KinesisConnectorConfiguration.PROP_S3_ENDPOINT, s3Endpoint)
    props.setProperty(KinesisConnectorConfiguration.PROP_S3_BUCKET, bucket)

    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_BYTE_SIZE_LIMIT, byteLimit)
    props.setProperty(KinesisConnectorConfiguration.PROP_BUFFER_RECORD_COUNT_LIMIT, recordLimit)

    props.setProperty(KinesisConnectorConfiguration.PROP_MAX_RECORDS, "10000")

    new KinesisConnectorConfiguration(props, new DefaultAWSCredentialsProviderChain())
  }

}
