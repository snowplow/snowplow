package com.snowplowanalytics.snowplow.sinks

// Snowplow Thrift
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  KinesisConnectorExecutorBase,
  KinesisConnectorRecordProcessorFactory
}

/**
 * Boilerplate class for Kinessis Conenector
 */
class S3SinkExecutor(config: KinesisConnectorConfiguration) extends KinesisConnectorExecutorBase[ SnowplowRawEvent, Array[Byte] ] {
  super.initialize(config)

  override def getKinesisConnectorRecordProcessorFactory: KinesisConnectorRecordProcessorFactory[ SnowplowRawEvent, Array[Byte] ] = {
    new KinesisConnectorRecordProcessorFactory[ SnowplowRawEvent, Array[Byte] ](new S3Pipeline(), config)
  }

}
