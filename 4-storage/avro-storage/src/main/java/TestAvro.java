package com.snowplowanalytics.snowplow.storage.avro;

import com.snowplowanalytics.snowplow.storage.avro.schema.User;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import org.apache.avro.file.*;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import java.io.*;



public class TestAvro {
	
	void RunTest1() {
		User user1 = new User();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		System.out.println("Hello World!");

		// alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder()
             .setName("Charlie")
             .setFavoriteColor("blue")
             .setFavoriteNumber(null)
             .build();

        // Serialize user1 and user2 to disk
		File file = new File("users.avro");
		DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
		DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
		try {
			dataFileWriter.create(user1.getSchema(), new File("users.avro"));
			dataFileWriter.append(user1);
			dataFileWriter.append(user2);
			dataFileWriter.append(user3);
			dataFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	

		// Deserialize Users from disk
		try {
			DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
			DataFileReader<User> dataFileReader = new DataFileReader<User>(file, userDatumReader);
			User user = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println(user);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	void RunTest2() {
		try {
			Schema schema = new Parser().parse(new File("src/main/avro/user.avsc"));
			GenericRecord user1 = new GenericData.Record(schema);
			user1.put("name", "Alyssa");
			user1.put("favorite_number", 256);
			// Leave favorite color null

			GenericRecord user2 = new GenericData.Record(schema);
			user2.put("name", "Ben");
			user2.put("favorite_number", 7);
			user2.put("favorite_color", "red");

			// Serialize user1 and user2 to disk
			File file = new File("users_without_code.avro");
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(schema, file);
			dataFileWriter.append(user1);
			dataFileWriter.append(user2);
			dataFileWriter.close();

			// Deserialize users from disk
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
			GenericRecord user = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				user = dataFileReader.next(user);
				System.out.println(user);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}
}