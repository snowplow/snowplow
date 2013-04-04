package com.snowplowanalytics.snowplow.storage.avro;

import com.snowplowanalytics.snowplow.storage.avro.schema.user;


public class TestUser {
	int i;

	public static void run() {
		User user1 = new user();
		user1.setName("Alyssa");
		user1.setFavoriteNumber(256);
		// Leave favorite color null

		// alternate constructor
		User user2 = new User("Ben", 7, "red");

		// Construct via builder
		User user3 = User.newBuilder()
             .setName("Charlie")
             .setFavoriteColor("blue")
             .setFavoriteNumber(null)
             .build();
	}
}