name := "spark-common"

version := "0.1"

scalaVersion := "2.12.15"

val Versions = new {
  val spark = "3.2.0"
  val scalaLogging = "3.9.4"
  val config = "1.4.1"
}

libraryDependencies += "org.apache.spark" %% "spark-core" % Versions.spark % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % Versions.spark % Provided
libraryDependencies += "org.scalatest" %% "scalatest" % Versions.spark % Test
libraryDependencies += "org.apache.spark" %% "spark-hive" % Versions.spark % Provided
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
libraryDependencies += "com.typesafe" % "config" % Versions.config


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

