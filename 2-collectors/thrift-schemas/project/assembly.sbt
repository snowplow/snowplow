resolvers += "bigtoast-github" at "http://bigtoast.github.com/repo/"

addSbtPlugin("com.github.bigtoast" % "sbt-thrift" % "0.7")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4"
