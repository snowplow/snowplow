package com.snowplowanalytics.snowplow.storage.avro

import test.example.User
import org.apache.avro.io._
import org.apache.avro.specific._
import org.apache.avro.file._
import org.apache.avro.Schema.Parser
import org.apache.avro.Schema
import org.apache.avro.generic._
import java.io._

class TestAvroScala {
	def RunTest1() {
		println("Running first Scala tests: serializing and deserializing with code generation")
		println("User 1 = Harry, favorite number 100")
		val user1 = new test.example.User()
		user1.setName("Harry")
		user1.setFavoriteNumber(100)
		// Leave favorite color null

		// Alternate constructor
		println("User 2 = Harold, favorite number 1, favorite colour blue")
		val user2 = new test.example.User("Harold", 1, "blue");

		// Cannot work out how to translate builder notation so that it works in Scala

		// Serialize user1 and user2 to disk
		val file = new File("users_scala.avro")
		var userDatumWriter = new SpecificDatumWriter(classOf[test.example.User])
		var dataFileWriter = new DataFileWriter(userDatumWriter)

		try {
			dataFileWriter.create(user1.getSchema, file)
			dataFileWriter.append(user1)
			dataFileWriter.append(user2)
			dataFileWriter.close
		} catch {
      		case e: IOException => { e.printStackTrace }
    	}

    	// Deserialize User from disk
    	try {
    		var userDatumReader = new SpecificDatumReader(classOf[test.example.User])
    		var dataFileReader = new DataFileReader(file, userDatumReader)
    		var user:test.example.User = null
    		while (dataFileReader.hasNext()) {
    			// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user)
				println(user)
    		}
    	} catch {
      		case e: IOException => { e.printStackTrace }
    	}	
	}

	def RunTest2() {
		try {
			println("Running second Scala tests: serializing and deserializing without code genearation...")

			val schema = new Parser().parse(new File("src/main/avro/testing/user.avsc"))

			println("Gertrud likes 64")
			val user1 = new GenericData.Record(schema)
			user1.put("name", "Gertrud")
			user1.put("favorite_number", 64)
			// Leave favorite color null

			val user2 = new GenericData.Record(schema)
			user2.put("name", "Graham")
			user2.put("favorite_number", 55)
			user2.put("favorite_color", "turquoise")

			// Serialize to disk
			val file = new File("users_without_code_scala.avro")
			val datumWriter = new GenericDatumWriter[GenericRecord](schema)
			val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
			dataFileWriter.create(schema, file)
			dataFileWriter.append(user1)
			dataFileWriter.append(user2)
			dataFileWriter.close

			// Deserialize users from disk
			val datumReader = new GenericDatumReader[GenericRecord](schema)
			val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)
			var user:GenericRecord = null
			while (dataFileReader.hasNext()) {
				user = dataFileReader.next(user)
				println(user)
			}
		} catch {
      		case e: IOException => { e.printStackTrace }
    	}
	}
}