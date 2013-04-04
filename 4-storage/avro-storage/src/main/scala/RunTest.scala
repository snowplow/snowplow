package com.snowplowanalytics.snowplow.storage.avro

object TestRunner {
    def main(args: Array[String]) {
      println("Running test...")

      var u = new TestUser
      u.i = 3;
      println(u.i + " value retrieved from instance of TestUser class")
      u.run();

      println("Finished...")

    }
  }