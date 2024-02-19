ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "Grad209-GDA-Hackathon"
  )

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
  "org.apache.spark" %% "spark-core" % "3.3.4",
  "org.apache.spark" %% "spark-sql" % "3.3.4"
)