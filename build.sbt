val commonSettings = Seq(
  version := "0.1.0-SNAPSHOT",
  organization := "com.ataraxer",
  homepage := Some(url("http://github.com/ataraxer/ottla")),
  licenses := Seq("MIT License" -> url(
    "http://www.opensource.org/licenses/mit-license.php")),
  scalaVersion := "2.10.4",
  scalacOptions ++= Seq(
    "-g:vars",
    "-deprecation",
    "-unchecked",
    "-feature",
    "-Xlint",
    "-Xfatal-warnings"),
  resolvers ++= Seq(
    "Ataraxer Nexus" at "http://nexus.ataraxer.com/repo/releases")) ++
  instrumentSettings

val dependencies = Seq(
  libraryDependencies ++= Seq(
    "org.apache.kafka" %% "kafka" % "0.8.1.1",
    "com.ataraxer" %% "zooowner-actor" % "0.2.4",
    "com.ataraxer" %% "akkit" % "0.1.0",
    "com.typesafe.akka" %% "akka-actor"   % "2.3.4",
    "com.typesafe.akka" %% "akka-testkit" % "2.3.4" % "test",
    "org.json4s" %% "json4s-native" % "3.2.10",
    "org.scalatest" %% "scalatest" % "2.2.0" % "test"))

val publishingSettings = Seq(
  publishTo <<= version { (ver: String) =>
    val nexus = "http://nexus.ataraxer.com/nexus/"
    if (ver.trim.endsWith("SNAPSHOT")) {
      Some("Snapshots" at nexus + "content/repositories/snapshots")
    } else {
      Some("Releases"  at nexus + "content/repositories/releases")
    }
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { case _ => false },
  pomExtra := (
    <scm>
      <url>git@github.com:ataraxer/ottla.git</url>
      <connection>scm:git:git@github.com:ataraxer/ottla.git</connection>
    </scm>
    <developers>
      <developer>
        <id>ataraxer</id>
        <name>Anton Karamanov</name>
        <url>github.com/ataraxer</url>
      </developer>
    </developers>))

lazy val ottla = project.in(file("."))
  .aggregate(ottlaCore)

lazy val ottlaCore = project.in(file("ottla-core"))
  .settings(name := "ottla-core")
  .settings(commonSettings: _*)
  .settings(publishingSettings: _*)
  .settings(dependencies: _*)
