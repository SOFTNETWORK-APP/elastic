import app.softnetwork.*

/////////////////////////////////
// Defaults
/////////////////////////////////

lazy val scala213 = "2.13.16"
lazy val javacCompilerVersion = "17"
lazy val scalacCompilerOptions = Seq(
  "-deprecation",
  "-feature",
  "-target:jvm-1.8"
)

ThisBuild / organization := "app.softnetwork"

name := "elastic"

ThisBuild / version := Versions.elasticSearch

ThisBuild / scalaVersion := scala213

ThisBuild / dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson,
  "com.github.jnr" % "jnr-ffi" % "2.2.17",
  "com.github.jnr" % "jffi"    % "1.3.13" classifier "native",
  "org.lmdbjava" % "lmdbjava" % "0.9.1" exclude("org.slf4j", "slf4j-api"),
)

lazy val moduleSettings = Seq(
  crossScalaVersions := Seq(scala213),
  scalacOptions ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => scalacCompilerOptions :+ "-Ypartial-unification"
      case Some((2, 13)) => scalacCompilerOptions
      case _             => Seq.empty
    }
  }
)

ThisBuild / javacOptions ++= Seq("-source", javacCompilerVersion, "-target", javacCompilerVersion)

ThisBuild / resolvers ++= Seq(
  "Softnetwork Server" at "https://softnetwork.jfrog.io/artifactory/releases/",
  "Softnetwork Snapshots" at "https://softnetwork.jfrog.io/artifactory/snapshots/",
  "Maven Central Server" at "https://repo1.maven.org/maven2",
  "Typesafe Server" at "https://repo.typesafe.com/typesafe/releases"
)

val logging = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging,
  "org.log4s"                  %% "log4s"         % Versions.log4s,
  "org.slf4j"                  % "slf4j-api"      % Versions.slf4j,
  "org.slf4j"                  % "jcl-over-slf4j" % Versions.slf4j,
  "org.slf4j"                  % "jul-to-slf4j"   % Versions.slf4j
)

val jacksonExclusions = Seq(
  ExclusionRule(organization = "com.fasterxml.jackson.core"),
  ExclusionRule(organization = "com.fasterxml.jackson.dataformat"),
  ExclusionRule(organization = "com.fasterxml.jackson.datatype"),
  ExclusionRule(organization = "com.fasterxml.jackson.module"),
  ExclusionRule(organization = "org.codehaus.jackson")
)

val json4s = Seq(
  "org.json4s" %% "json4s-jackson" % Versions.json4s,
  "org.json4s" %% "json4s-ext"     % Versions.json4s
).map(_.excludeAll(jacksonExclusions: _*))

ThisBuild / libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
)// ++ configDependencies ++ json4s ++ logging

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

Test / parallelExecution := false

lazy val sql = project.in(file("sql"))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)

lazy val client = project.in(file("client"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )
  .dependsOn(
    sql % "compile->compile;test->test;it->it"
  )

lazy val persistence = project.in(file("persistence"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )
  .dependsOn(
    client % "compile->compile;test->test;it->it"
  )

lazy val java = project.in(file("java"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    moduleSettings
  )
  .dependsOn(
    persistence % "compile->compile;test->test;it->it"
  )

lazy val testKit = project.in(file("testkit"))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    app.softnetwork.Info.infoSettings,
    moduleSettings
  )
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(
    java % "compile->compile;test->test;it->it"
  )

lazy val root = project.in(file("."))
  .configs(IntegrationTest)
  .settings(
    Defaults.itSettings,
    Publish.noPublishSettings,
    crossScalaVersions := Nil
  )
  .aggregate(sql, client, persistence, java, testKit)
