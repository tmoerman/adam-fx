organization := "org.tmoerman"

name := "adam-fx"

homepage := Some(url(s"https://github.com/tmoerman/"+name.value))

scalaVersion := "2.10.4"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.1" % "provided"

libraryDependencies += "org.bdgenomics.adam" % "adam-core_2.10" % "0.17.+" exclude("org.apache.hadoop", "*")

libraryDependencies += "org.bdgenomics.adam" % "adam-apis_2.10" % "0.17.+"

libraryDependencies += "org.bdgenomics.bdg-formats" % "bdg-formats" % "0.4.0"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.6"

libraryDependencies += "com.github.samtools" % "htsjdk" % "1.133"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.bdgenomics.utils" % "utils-misc_2.10" % "0.2.2" exclude("org.apache.spark", "*")

fork in run := true

// bintray-sbt plugin properties

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintrayPackageLabels := Seq("scala", "adam", "genomics", "snpeff", "variants")

pomExtra :=
  <scm>
    <url>git@github.com:tmoerman/{name.value}.git</url>
    <connection>scm:git:git@github.com:tmoerman/{name.value}.git</connection>
  </scm>
  <developers>
    <developer>
      <id>tmoerman</id>
      <name>tmoerman</name>
      <url>https://github.com/tmoerman</url>
    </developer>
  </developers>

// sbt-release properties

releaseCrossBuild := false

// releaseNextVersion := { ver => sbtrelease.Version(ver).map(_.bumpMinor.string).getOrElse(sbtrelease.versionFormatError) }