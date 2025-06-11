import sbt.Keys.{libraryDependencies, scalaVersion, version}


lazy val root = (project in file(".")).
  settings(
    name := "CSE512-Hotspot-Analysis-Template",

    version := "0.1.0",

    scalaVersion := "2.12.1",

    organization  := "org.datasyslab",

    publishMavenStyle := true,

    mainClass := Some("cse512.Entrance")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "org.specs2" %% "specs2-core" % "4.19.1" % "test",
  "org.specs2" %% "specs2-junit" % "4.19.1" % "test"
)