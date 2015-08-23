name := """adam-fx"""

version := "0.1.0"

scalaVersion := "2.10.4"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

publishMavenStyle := true

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-common" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.3.1"

libraryDependencies += "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.1-SNAPSHOT"

libraryDependencies += "org.bdgenomics.adam" % "adam-apis_2.10" % "0.17.1-SNAPSHOT"

libraryDependencies += "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.6"

libraryDependencies += "com.github.samtools" % "htsjdk" % "1.133"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.bdgenomics.utils" % "utils-misc_2.10" % "0.2.2" exclude("org.apache.spark", "*")

fork in run := true