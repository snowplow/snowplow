package com.snowplowanalytics.snowplow.storage.avro

object TestRunner {
    def main(args: Array[String]) {
      println("Running test...")

      var u = new TestAvro
      u.RunTest1();

      println("Finished...")

    }
  }