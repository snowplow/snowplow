package com.snowplowanalytics.snowplow.storage.avro

object TestRunner {
    def main(args: Array[String]) {
      println("Running test...")

      var u = new TestAvro
      u.RunTest1()
      println("Run test 1 - now going to run test 2")

      u.RunTest2()

      println("Finished...")

      var v = new TestAvroScala
      v.RunTest1
      v.RunTest2

    }
  }