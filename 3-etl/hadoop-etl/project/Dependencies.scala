/*
 * Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._

object Dependencies {
	val resolutionRepos = Seq(
		ScalaToolsSnapshots,
		"Concurrent Maven Repo" at "http://conjars.org/repo", // For Scalding, Cascading etc
		"Twitter Maven Repo" at "http://maven.twttr.com/" // For "wonderful" util functions
	)

	object Urls {
		val maxmindJava = "http://www.maxmind.com/download/geoip/api/java/GeoIPJava-%s.zip"
		val maxmindData = "http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz"
	}

	object V {
		val maxmind     = "1.2.8" // Compiled in BuildSettings
		val scalding    = "0.7.3"
		val collUtils   = "5.3.10"
		val specs2      = "1.8"
		// Add versions for your additional libraries here...
	}

	object Libraries {
		val scalding    = "com.twitter"                %% "scalding"            % V.scalding
		val collUtils	  = "com.twitter"								 %  "util-collection"		  % V.collUtils
		val specs2      = "org.specs2"                 %% "specs2"              % V.specs2       % "test"
		// Add additional libraries from mvnrepository.com (SBT syntax) here...
	}
}