ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

lazy val root = (project in file("."))
  .settings(
    name := "ScalaWebApp"
  )

val AkkaVersion = "2.7.0"
val AkkaHttpVersion = "10.5.0"
val sparkVersion = "3.1.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.postgresql" % "postgresql" % "42.5.1"
)
