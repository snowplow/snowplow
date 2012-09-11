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

// SBT
import sbt._
import Keys._

object BuildSettings {

	// Basic settings for our app
	lazy val basicSettings = Seq[Setting[_]](
		organization  := "SnowPlow Analytics Ltd",
		version       := "0.0.1",
		description   := "The SnowPlow Hadoop ETL process, written in Scalding",
		scalaVersion  := "2.9.1",
		scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
		resolvers     ++= Dependencies.resolutionRepos
	)

	// For MaxMind support
	import Dependencies._
	lazy val maxmindSettings = Seq(

		// Download and compile the MaxMind GeoIP Java API from source
		// Adapted from https://github.com/guardian/maxmind-geoip-build/blob/master/project/Build.scala
		sourceGenerators in Compile <+= (sourceManaged in Compile) map { out =>
			val zip = new URL(Urls.maxmindJava format (V.maxmind))
			IO.unzipURL(zip, out)
			(out / "GeoIPJava-%s".format(V.maxmind) / "source" ** ("*.java")).get
		},

		// Download the GeoLite City and add it into our jar
		resourceGenerators in Compile <+= (resourceManaged in Compile) map { out =>
			val gzRemote = new URL(Urls.maxmindData)
			val datLocal = out / "maxmind" / "GeoLiteCity.dat"
			
			// Only fetch if we don't already have it (because MaxMind 403s if you download GeoIP.dat.gz too frequently)
			if (!datLocal.exists()) {
				// TODO: replace this with simply IO.gunzipURL(gzRemote, out / "maxmind") when https://github.com/harrah/xsbt/issues/529 implemented
				val gzLocal = out / "GeoLiteCity.dat.gz"        
				IO.download(gzRemote, gzLocal)
				IO.createDirectory(out / "maxmind")
				IO.gunzip(gzLocal, datLocal)
				IO.delete(gzLocal)
				// gunzipURL(gzRemote, out / "maxmind")
			}
			datLocal.get
		}
	)

	// sbt-assembly settings for building a fat jar
	import sbtassembly.Plugin._
	import AssemblyKeys._
	lazy val sbtAssemblySettings = assemblySettings ++ Seq(

		// Slightly cleaner jar name
		jarName in assembly <<= (name, version) { (name, version) => name + "-" + version + ".jar" },
		
		// Store in the upload folder for the ETL process to upload as necessary
		target in assembly <<= (target) { (target) => target / ".." / "upload" },

		// Drop these jars
		excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
			val excludes = Set(
				"jsp-api-2.1-6.1.14.jar",
				"jsp-2.1-6.1.14.jar",
				"jasper-compiler-5.5.12.jar",
				"janino-2.5.16.jar", // Janino includes a broken signature, and is not needed anyway
				"commons-beanutils-core-1.8.0.jar", // Clash with each other and with commons-collections
				"commons-beanutils-1.7.0.jar",      // "
				"hadoop-core-0.20.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
				"hadoop-tools-0.20.2.jar" // "
			) 
			cp filter { jar => excludes(jar.data.getName) }
		},
		
		mergeStrategy in assembly <<= (mergeStrategy in assembly) {
			(old) => {
				case "project.clj" => MergeStrategy.discard // Leiningen build files
				case x => old(x)
			}
		}
	)

	lazy val buildSettings = basicSettings ++ maxmindSettings ++ sbtAssemblySettings
}