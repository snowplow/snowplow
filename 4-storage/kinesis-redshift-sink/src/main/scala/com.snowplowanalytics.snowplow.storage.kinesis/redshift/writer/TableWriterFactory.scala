package com.snowplowanalytics.snowplow.storage.kinesis.redshift.writer

import javax.sql.DataSource

import com.snowplowanalytics.iglu.client.SchemaKey

/**
 * Created by denismo on 28/09/15.
 */
trait TableWriterFactory {
  def newWriter(dataSource: DataSource, schema: SchemaKey, table: String): CopyTableWriter
  def newEventsWriter(dataSource: DataSource, table: String): CopyTableWriter
}
