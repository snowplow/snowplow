package com.snowplowanalytics.snowplow.storage.avro

import com.snowplowanalytics.snowplow.storage.avro.schema.User
import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.file._
import org.apache.avro.Schema.Parser
import org.apache.avro.Schema
import org.apache.avro.generic._
import java.io._

class TestAvroScala {
	def RunTest1() {
		val user1 = new schema.User()
		user1.setName("Harry")
		user1.setFavoriteNumber(100)
	}
}