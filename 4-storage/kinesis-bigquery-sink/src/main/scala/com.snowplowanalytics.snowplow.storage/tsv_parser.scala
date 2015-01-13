import scala.io.Source

object TSVParser extends App {
  if (args.length > 0) {
      
    for (line <- Source.fromFile(args(0)).getLines()){
      var split_text = line.split("\t", -1).toList
      println("split_text", split_text)
      println("split_text(0)", split_text(0))
      println("length", split_text.length)
    }
    //println(line.length + " " + line)
  } else {
    Console.err.println("Please enter filename")
  }
}
