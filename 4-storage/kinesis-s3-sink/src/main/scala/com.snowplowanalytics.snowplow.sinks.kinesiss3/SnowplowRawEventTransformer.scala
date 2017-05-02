package com.snowplowanalytics.snowplow.sinks

// AWS libs
import com.amazonaws.services.kinesis.model.Record

// AWS Kinesis Connector libs
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer

// Thrift libs
import org.apache.thrift.{TSerializer,TDeserializer}

// Snowplow thrift
import com.snowplowanalytics.snowplow.collectors.thrift.SnowplowRawEvent

/**
 * Thrift serializer/deserializer class
 */
class SnowplowRawEventTransformer extends ITransformer[ SnowplowRawEvent, SnowplowRawEvent ] {
  lazy val serializer = new TSerializer()
  lazy val deserializer = new TDeserializer()

  override def toClass(record: Record): SnowplowRawEvent = {
    var obj = new SnowplowRawEvent()
    deserializer.deserialize(obj, record.getData().array())
    obj
  }

  override def fromClass(record: SnowplowRawEvent) = record
}
