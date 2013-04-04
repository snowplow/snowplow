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
		println("Running Scala tests")
		println("User 1 = Harry, favorite number 100")
		val user1 = new schema.User()
		user1.setName("Harry")
		user1.setFavoriteNumber(100)
		// Leave favorite color null

		// Alternate constructor
		println("User 2 = Harold, favorite number 1, favorite colour blue")
		val user2 = new schema.User("Harold", 1, "blue");

		// Cannot work out how to translate builder notation so that it works in Scala

		// Serialize user1 and user2 to disk
		val file = new File("users_scala.avro")
		var userDatumWriter = new SpecificDatumWriter(classOf[schema.User])
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
    		var userDatumReader = new SpecificDatumReader(classOf[schema.User])
    		var dataFileReader = new DataFileReader(file, userDatumReader)
    		var user:schema.User = null
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
}