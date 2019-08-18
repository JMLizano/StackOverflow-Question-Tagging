name := "stackoverflowQuestions"
organization := "com.jmlizano"
version := "0.1"
scalaVersion := "2.12.8"

// Spark Information
val sparkVersion = "2.4.3"
lazy val root = (project in file(".")).enablePlugins(PlayScala)

// allows us to include spark packages
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies += guice
libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core"   % sparkVersion,
  "org.apache.spark" %% "spark-sql"    % sparkVersion,

  // Necessary to avoid a problem with guava
  // https://stackoverflow.com/questions/48590083/guava-dependency-error-for-spark-play-framework-using-scala
  "org.apache.hadoop" % "hadoop-client" % "2.7.2",

  // spark-modules
   "org.apache.spark" %% "spark-mllib" % sparkVersion,

  // third-party
  "com.databricks" %% "spark-xml" % "0.6.0",

  // testing
  "org.scalatest"  %% "scalatest"  % "3.2.0-SNAP10" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.0"       % Test,
  "org.scalatestplus.play" %% "scalatestplus-play" % "4.0.3" % Test,

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)
dependencyOverrides ++= Seq("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.2")

testOptions in Test += Tests.Argument("-oFD")
parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")