scalaVersion          :=  "2.10.4"
scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8", "-unchecked", "-feature", "-target:jvm-1.7")
scalacOptions in Test :=  Seq("-Yrangepos")
