name := """adam-effects"""

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1"

libraryDependencies += "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.1-SNAPSHOT"

libraryDependencies += "org.bdgenomics.adam" % "adam-apis_2.10" % "0.17.1-SNAPSHOT"

libraryDependencies += "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

fork in run := true