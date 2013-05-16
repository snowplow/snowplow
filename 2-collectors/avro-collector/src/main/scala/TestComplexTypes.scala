package com.snowplowanalytics.snowplow.storage.avro

import test.example._
import org.apache.avro.specific._
import org.apache.avro.file._
import java.io._


class TestComplexTypes {
  def RunTest1 {
    println("Creating a new human")
    println("Name: Alfred")
    println("Sex: Male")
    println("Date of birth: 4th March 1976")

    val human1 = new Human
    human1.setName("Alfred")
    human1.setDateOfBirth("4th March 1976")
    human1.setSex(Sex.male)
    val ajob = new Job("manager", "agriculture", 50000)
    human1.setJob(ajob)

    println("human1.get(0) " +  human1.get(0))
    println("human1.get(1) " +  human1.get(1))
    println("human1.get(2) " +  human1.get(2))
    println("human1.get(3) " +  human1.get(3))


    // Now serialize Alfred
    val file = new File("humans.avro")
    val humanDatumWriter = new SpecificDatumWriter(classOf[Human])
    val dataFileWriter = new DataFileWriter(humanDatumWriter)

    try {
      dataFileWriter.create(human1.getSchema, file)
      dataFileWriter.append(human1)
      dataFileWriter.close
    } catch {
    case e: IOException => { e.printStackTrace }
    }

    println("Finished serializing Alfred")

    // Now deserialize Alfred
    val humanDatumReader = new SpecificDatumReader(classOf[Human])
    val dataFileReader = new DataFileReader(file, humanDatumReader)
    var human:Human = null

    try {
      while (dataFileReader.hasNext) {
        human = dataFileReader.next(human)
        println(human)
      }
    } catch {
      case e: IOException => { e.printStackTrace }
    }


  }
}
