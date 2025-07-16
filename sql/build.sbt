organization := "app.softnetwork.elastic"

name := "elastic-sql"

val jackson = Seq(
  "com.fasterxml.jackson.core"   % "jackson-databind"          % Versions.jackson,
  "com.fasterxml.jackson.core"   % "jackson-core"              % Versions.jackson,
  "com.fasterxml.jackson.core"   % "jackson-annotations"       % Versions.jackson,
  "com.fasterxml.jackson.dataformat"   % "jackson-dataformat-cbor"    % Versions.jackson,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml"   % Versions.jackson,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8"   % Versions.jackson,
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % Versions.jackson,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % Versions.jackson,
  "com.fasterxml.jackson.module" % "jackson-module-parameter-names" % Versions.jackson,
  "com.fasterxml.jackson.module" %% "jackson-module-scala"    % Versions.jackson,
)

val elastic4s = Seq(
  "nl.gn0s1s" %% "elastic4s-core"     % Versions.elastic4s exclude ("org.elasticsearch", "elasticsearch") exclude("org.slf4j", "slf4j-api"),
)

val scalatest = Seq(
  "org.scalatest" %% "scalatest" % Versions.scalatest  % Test
)

libraryDependencies ++= jackson ++ elastic4s ++ scalatest ++ Seq(
  "javax.activation" % "activation" % "1.1.1" % Test
) :+
  "org.scala-lang" % "scala-reflect" % "2.12.18" :+
  "com.google.code.gson" % "gson" % Versions.gson % Test


