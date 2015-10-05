package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import java.util.Properties
import javax.sql.DataSource

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.snowplowanalytics.iglu.client.{Resolver, SchemaKey}
import scaldi.Injector
import scaldi.{Injector, Injectable}
import Injectable._

/**
 * Created by denismo on 28/09/15.
 */
class DefaultTableWriterFactory(implicit injector: Injector) extends TableWriterFactory {
  override def newWriter(dataSource: DataSource, schema: SchemaKey, table: String): CopyTableWriter = new SchemaTableWriter(dataSource, schema, table)
  override def newEventsWriter(dataSource: DataSource, table: String): CopyTableWriter = new EventsTableWriter(dataSource, table)
}
