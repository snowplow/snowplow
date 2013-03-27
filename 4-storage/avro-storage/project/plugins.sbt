resolvers += Resolver.url("plugins-artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.5")

resolvers += "cavorite" at "http://files.cavorite.com/maven/"

addSbtPlugin("com.cavorite" % "sbt-avro" % "0.3.0")
