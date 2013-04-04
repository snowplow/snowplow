package com.snowplowanalytics.snowplow.storage.avro;

import com.snowplowanalytics.snowplow.storage.avro.schema.User;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;
import org.apache.avro.file.*;
import java.io.File;


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
		dataFileWriter.create(user1.getSchema(), new File("users.avro"));
		dataFileWriter.append(user1);
		dataFileWriter.append(user2);
		dataFileWriter.append(user3);
		dataFileWriter.close();
	}
}