ThisBuild / organization := "com.lab" // ignored on machines whose global.sbt sets organization; see projectID below
ThisBuild / version := "0.1.0-SNAPSHOT"

// Clearance are on Scala 2.13, so cross-build for both.
val scala213 = "2.13.18"
val scala3 = "3.3.8"

ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := Seq(scala213, scala3)

lazy val root = (project in file("."))
  .settings(
    // ~/.sbt/1.0/global.sbt sets `organization` per project after this file is read, so it would win over
    // `organization := ...`. Overriding projectID is what publishing and dependants actually use.
    projectID := projectID.value.withOrganization("com.lab"),
    name := "dualpool",
    description := "Read/write connection pool isolation for Slick services",
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % "3.6.1",
      "com.zaxxer" % "HikariCP" % "7.1.0" exclude("org.slf4j", "slf4j-api"),
      "com.typesafe" % "config" % "1.4.9",
      "org.slf4j" % "slf4j-api" % "2.0.17",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.hsqldb" % "hsqldb" % "2.7.4" % Test
    )
  )
