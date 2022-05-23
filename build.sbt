ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-word-count-per-record"
  )

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.1.0"

libraryDependencies += "org.slf4j" % "slf4j-reload4j" % "1.7.36"
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36"