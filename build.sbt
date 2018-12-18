name := "stackoverflowQuestions"
version := "0.1"
scalaVersion := "2.11.11"

// Spark Information
val sparkVersion = "2.2.0"

// allows us to include spark packages
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"


libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core"   % sparkVersion,
  "org.apache.spark" %% "spark-sql"    % sparkVersion,

  // AWS Dependencies
  "org.apache.hadoop" % "hadoop-aws"   % "2.8.3",
  "com.amazonaws"     % "aws-java-sdk" % "1.11.448",

  // spark-modules
   "org.apache.spark" %% "spark-mllib" % sparkVersion,

  // third-party
  "com.databricks" % "spark-xml_2.11" % "0.4.1",

  // testing
  "org.scalatest"  %% "scalatest"  % "3.2.0-SNAP10" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.0"       % "test",

  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
)

testOptions in Test += Tests.Argument("-oFD")
parallelExecution in Test := false
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")