name := "ExtractTransformLoad"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVersion = "2.4.7"

val json4sJackson = "org.json4s" %% "json4s-jackson" % "{latestVersion}"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

