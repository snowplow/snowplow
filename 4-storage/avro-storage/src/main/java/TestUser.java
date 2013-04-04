package com.snowplowanalytics.snowplow.storage.avro;

import com.snowplowanalytics.snowplow.storage.avro.schema.User;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import org.apache.avro.file.*;
import java.io.*;


public class TestUser {
	int i;

	void run() {
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
}