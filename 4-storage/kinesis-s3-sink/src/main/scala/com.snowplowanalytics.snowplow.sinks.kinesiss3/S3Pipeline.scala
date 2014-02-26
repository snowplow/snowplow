package com.snowplowanalytics.snowplow.sinks

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.{
  IEmitter,
  IBuffer,
  ITransformer,
  IFilter,
  IKinesisConnectorPipeline
}
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.impl.{BasicMemoryBuffer,AllPassFilter}

// Snowplow Thrift
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

/**
 * S3Pipeline class sets up the Emitter/Buffer/Transformer/Filter
 */
class S3Pipeline extends IKinesisConnectorPipeline[ SnowplowRawEvent, Array[Byte] ] {

  override def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[ Array[Byte] ] = {
    new S3Emitter(configuration)
  }

  override def getBuffer(configuration: KinesisConnectorConfiguration): IBuffer[SnowplowRawEvent] = {
    new BasicMemoryBuffer[SnowplowRawEvent](configuration)
  }

  override def getTransformer(configuration: KinesisConnectorConfiguration): ITransformer[ SnowplowRawEvent, Array[Byte] ] = {
    new SnowplowRawEventTransformer()
  }

  override def getFilter(configuration: KinesisConnectorConfiguration): IFilter[ SnowplowRawEvent ] = {
    new AllPassFilter[SnowplowRawEvent]()
  }

}

